import os
import pickle
import re
from concurrent.futures.thread import ThreadPoolExecutor
import time
from random import randrange, seed

from ibm_botocore.exceptions import ClientError
from pandas import read_csv, DataFrame, Series, cut, concat
from queue import Queue, Empty
import numpy as np
from numpy import empty, array, argsort, searchsorted, concatenate, take, full, iinfo, finfo, dtype
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.patch import patch_map_monitor, unpatch_map_monitor
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _upload_progress, _check_end_signal, \
    _check_if_speculative
from pywren_ibm_cloud.sort.tests.basic_sort import _partition_into_segments_basic

from pywren_ibm_cloud.sort.utils import _extract_real_bounds_csv, get_normalized_type_names, correct_types
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE, DEFAULT_GRANULE_SIZE_REDUCE, \
    GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION, DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN


# Monitor option
# _partition_into_segments_monitored
def _partition_into_segments_standalone_hadoop(args, ibm_cos):
    range_i = args['index']
    start_time = time.time()

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['parser_info_bucket'],
                                                  Key=args['parser_info_path'])['Body'].read())

    print("Initial read at {}".format(time.time() - start_time))

    input_bucket = parser_data['input_bucket']
    output_bucket = parser_data['output_bucket']
    input_path = parser_data['input_path']
    intermediate_path = parser_data['intermediate_path']
    granule_size = parser_data['granularity']
    partition_bucket = parser_data['partition_bucket']
    partition_file_path = parser_data['partition_file_path']
    chunk_range_bucket = parser_data['chunk_range_bucket']
    chunk_range_file_path = parser_data['chunk_range_file_path']
    num_workers_phase1 = parser_data['num_workers_phase1']
    total_size = parser_data['total_size']
    delimiter = parser_data['delimiter']

    # Adapt
    my_output_path = "{}/{}".format(intermediate_path, range_i)

    partition_ranges = pickle.loads(ibm_cos.get_object(Bucket=chunk_range_bucket,
                                                       Key=chunk_range_file_path)['Body'].read())

    lower_bound = partition_ranges[range_i]
    upper_bound = partition_ranges[range_i + 1]
    lower_bound, upper_bound = _extract_real_bounds_csv(ibm_cos, lower_bound, upper_bound, total_size,
                                                        input_bucket,
                                                        input_path)

    print("started classification of chunk {} ({} to {} - {})".format(range_i,
                                                                      lower_bound,
                                                                      upper_bound,
                                                                      upper_bound - lower_bound))
    # Read segments' file from COS
    segm_lower_bounds = pickle.loads(ibm_cos.get_object(Bucket=partition_bucket,
                                                        Key=partition_file_path)['Body'].read())
    print("Got segment info from COS *********************")

    nms = ['0', '1', '2']
    parser_data = dict()
    parser_data['column_numbers'] = [1]
    parser_data['dtypes'] = ['int32', 'float64', 'float32']

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]
    nms_without_key = nms.copy()
    nms_without_key.remove(nms[parser_data['column_numbers'][0]])
    bins = segm_lower_bounds
    total_bytes = upper_bound - lower_bound
    print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))
    chunk_size = int(granule_size * (1024 ** 2))
    read_bounds = list(range(lower_bound, upper_bound, chunk_size))
    read_bounds.append(upper_bound)
    for i in range(len(read_bounds) - 1):
        lb, ub = _extract_real_bounds_csv(ibm_cos, read_bounds[i], read_bounds[i + 1], total_size, input_bucket,
                                          input_path)
        read_bounds[i] = lb

    chunk_name_queue = Queue()
    for i in range(len(read_bounds) - 1):
        chunk_name_queue.put_nowait(i)
    chunk_queue = Queue()
    segment_queue = Queue()
    num_chunks = len(read_bounds) - 1

    def chunk_classifier():
        print("started chunk classifier\n")
        chunk_counter = 0
        indexes = Series(list(range(len(segm_lower_bounds) - 1)))
        total_rows = 0
        # Calculate rows per 64MB
        # A dataframe of 64MB per index
        df_buffers = [_allocate_dataframe(nms, types, granule_size)
                      for i in range(len(segm_lower_bounds))]
        last_rows = [0 for i in range(len(segm_lower_bounds))]
        part_counts = [0 for i in range(len(segm_lower_bounds))]
        row_counts = [0 for i in range(len(segm_lower_bounds))]
        while chunk_counter < num_chunks:

            df = chunk_queue.get(block=True, timeout=15)
            print("will classify {} rows".format(len(df)))
            key_series = df['1']
            # Classify rows into segments.
            classified_indexes = cut(key_series, bins, labels=indexes, include_lowest=True).values.add_categories(
                len(segm_lower_bounds) - 1)
            classified_indexes = np.array(classified_indexes.fillna(len(segm_lower_bounds) - 1))

            for i in range(len(segm_lower_bounds)):
                # extract segments' rows
                df_buffers[i], additional_buffers, last_rows[i], new_buffer = _copy_rows(df_buffers[i], last_rows[i],
                                                                                         df, i, classified_indexes)
                if new_buffer is not None:
                    # One or more buffers are full
                    # Get chunk name
                    chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, i, part_counts[i])
                    # put buffer into queue
                    segment_queue.put({'chunk': df_buffers[i],
                                       'name': chunk_name})
                    row_counts[i] += len(df_buffers[i])
                    total_rows += len(df_buffers[i])
                    # New buffer
                    df_buffers[i] = new_buffer
                    part_counts[i] += 1
                    for b in additional_buffers:
                        chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, i, part_counts[i])
                        segment_queue.put({'chunk': b,
                                           'name': chunk_name})
                        row_counts[i] += len(b)
                        total_rows += len(b)
                        part_counts[i] += 1
            chunk_counter += 1

        for s in range(len(segm_lower_bounds)):
            chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, s, part_counts[s])
            segment_queue.put({'chunk': df_buffers[s][0:last_rows[s]],
                               'name': chunk_name})
            row_counts[s] += last_rows[s]
            total_rows += last_rows[s]
            part_counts[s] += 1
        segment_queue.put(-1)
        upload_info = {'row_num': row_counts, 'nums': part_counts}
        return total_rows, upload_info

    def chunk_reader():
        while not chunk_name_queue.empty():
            c_i = chunk_name_queue.get()
            print("Reading parallel chunk {}".format(c_i))
            read_part = ibm_cos.get_object(Bucket=input_bucket,
                                           Key=input_path,
                                           Range=''.join(
                                               ['bytes=', str(read_bounds[c_i]), '-',
                                                str(read_bounds[c_i + 1] - 1)]))
            df = read_csv(read_part['Body'], engine='c', index_col=None, header=None,
                          names=nms,
                          delimiter=delimiter, dtype=types)
            print("read {} rows".format(len(df)))
            chunk_queue.put(df)

    def segment_chunk_writer():
        while True:
            next_segm_chunk = segment_queue.get(block=True, timeout=30)
            if next_segm_chunk is not -1:
                segment_path = next_segm_chunk['name']
                segment_chunk = next_segm_chunk['chunk']
                ibm_cos.put_object(Bucket=output_bucket,
                                   Key=segment_path,
                                   Body=pickle.dumps(segment_chunk))
                print("wrote {}".format(segment_path))
            else:
                return 0

    with ThreadPoolExecutor(3) as pool:
        pool.submit(chunk_reader)
        f2 = pool.submit(chunk_classifier)
        f3 = pool.submit(segment_chunk_writer)
        f3.result()
        total_rows_after, upload_info = f2.result()

    # Metadata upload
    ibm_cos.put_object(Bucket=output_bucket,
                       Key="{}/part_number.pickle".format(my_output_path),
                       Body=pickle.dumps(upload_info))

    # Upload manually output path dictionary entry, in speculative it is done
    # inside wait function.
    ibm_cos.put_object(Bucket=output_bucket,
                       Key="{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i),
                       Body=pickle.dumps(my_output_path))

    return total_rows_after

    ########################################################################################################################
    ########################################################################################################################


# Monitor option
# _partition_into_segments_monitored
def _partition_into_segments_monitored_hadoop(args, ibm_cos):
    range_i = args['arguments']['args']['index']
    start_time = time.time()

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['arguments']['args']['parser_info_bucket'],
                                                  Key=args['arguments']['args']['parser_info_path'])['Body'].read())

    print("Initial read at {}".format(time.time() - start_time))

    input_bucket = parser_data['input_bucket']
    output_bucket = parser_data['output_bucket']
    input_path = parser_data['input_path']
    intermediate_path = parser_data['intermediate_path']
    granule_size = parser_data['granularity']
    partition_bucket = parser_data['partition_bucket']
    partition_file_path = parser_data['partition_file_path']
    chunk_range_bucket = parser_data['chunk_range_bucket']
    chunk_range_file_path = parser_data['chunk_range_file_path']
    num_workers_phase1 = parser_data['num_workers_phase1']
    total_size = parser_data['total_size']
    delimiter = parser_data['delimiter']

    # Protocol to know if i am speculative.
    fname, is_speculative = _check_if_speculative(ibm_cos, args, int(range_i))
    if is_speculative:
        my_output_path = "{}/{}s".format(intermediate_path, range_i)
    else:
        my_output_path = "{}/{}".format(intermediate_path, range_i)

    partition_ranges = pickle.loads(ibm_cos.get_object(Bucket=chunk_range_bucket,
                                                       Key=chunk_range_file_path)['Body'].read())

    lower_bound = partition_ranges[range_i]
    upper_bound = partition_ranges[range_i + 1]
    lower_bound, upper_bound = _extract_real_bounds_csv(ibm_cos, lower_bound, upper_bound, total_size,
                                                        input_bucket,
                                                        input_path)

    print("started classification of chunk {} ({} to {} - {})".format(range_i,
                                                                      lower_bound,
                                                                      upper_bound,
                                                                      upper_bound - lower_bound))
    # Read segments' file from COS
    segm_lower_bounds = pickle.loads(ibm_cos.get_object(Bucket=partition_bucket,
                                                        Key=partition_file_path)['Body'].read())
    print("Got segment info from COS *********************")

    nms = ['0', '1', '2']
    parser_data = dict()
    parser_data['column_numbers'] = [1]
    parser_data['dtypes'] = ['int32', 'float64', 'float32']

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]
    nms_without_key = nms.copy()
    nms_without_key.remove(nms[parser_data['column_numbers'][0]])
    bins = segm_lower_bounds
    total_bytes = upper_bound - lower_bound
    print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))
    chunk_size = int(granule_size * (1024 ** 2))
    read_bounds = list(range(lower_bound, upper_bound, chunk_size))
    read_bounds.append(upper_bound)
    for i in range(len(read_bounds) - 1):
        lb, ub = _extract_real_bounds_csv(ibm_cos, read_bounds[i], read_bounds[i + 1], total_size, input_bucket,
                                          input_path)
        read_bounds[i] = lb

    chunk_name_queue = Queue()
    for i in range(len(read_bounds) - 1):
        chunk_name_queue.put_nowait(i)
    chunk_queue = Queue()
    segment_queue = Queue()
    finish_signal_queue= Queue()
    num_chunks = len(read_bounds) - 1
    print("{} chunks to read".format(num_chunks))

    def chunk_classifier():
        print("started chunk classifier\n")
        chunk_counter = 0
        indexes = Series(list(range(len(segm_lower_bounds) - 1)))
        total_rows = 0
        clas_rows = 0
        expected_row_num = total_bytes / _get_row_size(types)
        print("Expected row num {}".format(expected_row_num))
        print("Granularity {}".format(granule_size))
        # Calculate rows per 64MB
        # A dataframe of 64MB per index
        df_buffers = [_allocate_dataframe(nms, types, granule_size)
                      for i in range(len(segm_lower_bounds))]
        last_rows = [0 for i in range(len(segm_lower_bounds))]
        part_counts = [0 for i in range(len(segm_lower_bounds))]
        row_counts = [0 for i in range(len(segm_lower_bounds))]
        while chunk_counter < num_chunks:
            # print("new chunk")
            finish_signal = _check_end_signal(ibm_cos, args, fname)
            if finish_signal:
                finish_signal_queue.put(True)
                return None, None
            # print("waiting for chunk")
            df = chunk_queue.get(block=True, timeout=15)
            print("classifying chunk {}".format(chunk_counter))
            key_series = df['1']
            # Classify rows into segments.
            classified_indexes = cut(key_series, bins, labels=indexes, include_lowest=True).values.add_categories(
                len(segm_lower_bounds) - 1)
            classified_indexes = np.array(classified_indexes.fillna(len(segm_lower_bounds) - 1))
            clas_rows += len(classified_indexes)

            for i in range(len(segm_lower_bounds)):
                # extract segments' rows
                df_buffers[i], additional_buffers, last_rows[i], new_buffer = _copy_rows(df_buffers[i], last_rows[i],
                                                                                         df, i, classified_indexes)
                if new_buffer is not None:
                    # One or more buffers are full
                    # Get chunk name
                    chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, i, part_counts[i])
                    print("to write {}/sgm{}/gran{}.pickle".format(my_output_path, i, part_counts[i]))
                    # put buffer into queue
                    segment_queue.put({'chunk': df_buffers[i],
                                       'name': chunk_name})
                    row_counts[i] += len(df_buffers[i])
                    total_rows += len(df_buffers[i])
                    # New buffer
                    df_buffers[i] = new_buffer
                    part_counts[i] += 1
                    for b in additional_buffers:
                        chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, i, part_counts[i])
                        segment_queue.put({'chunk': b,
                                           'name': chunk_name})
                        row_counts[i] += len(b)
                        total_rows += len(b)
                        part_counts[i] += 1
            chunk_counter += 1

            if not is_speculative:
                _upload_progress(ibm_cos, range_i, clas_rows / expected_row_num, time.time() - start_time, args)

        for s in range(len(segm_lower_bounds)):
            chunk_name = "{}/sgm{}/gran{}.pickle".format(my_output_path, s, part_counts[s])
            print("to write {}/sgm{}/gran{}.pickle".format(my_output_path, s, part_counts[s]))
            segment_queue.put({'chunk': df_buffers[s][0:last_rows[s]],
                               'name': chunk_name})
            # print("{} objects in segment queue".format(segment_queue.qsize()))
            row_counts[s] += last_rows[s]
            total_rows += last_rows[s]
            part_counts[s] += 1
        segment_queue.put(-1)
        upload_info = {'row_num': row_counts, 'nums': part_counts}
        print("finishing classifier")
        return total_rows, upload_info

    def chunk_reader():
        while not chunk_name_queue.empty():
            c_i = chunk_name_queue.get()
            if not finish_signal_queue.empty():
                # received finish signal
                return None
            print("Reading chunk {}".format(c_i))
            read_part = ibm_cos.get_object(Bucket=input_bucket,
                                           Key=input_path,
                                           Range=''.join(
                                               ['bytes=', str(read_bounds[c_i]), '-',
                                                str(read_bounds[c_i + 1] - 1)]))
            df = read_csv(read_part['Body'], engine='c', index_col=None, header=None,
                          names=nms,
                          delimiter=delimiter, dtype=types)
            # print("read {} rows".format(len(df)))
            chunk_queue.put(df)
            time.sleep(0.001)
            # print("{} objects in chunk_queue".format(chunk_queue.qsize()))

    def segment_chunk_writer():
        print("Starting chunk writer")
        # max_trials = 4
        # num_trials = 0
        while True:
            try:
                next_segm_chunk = segment_queue.get(block=True, timeout=120)
            except Empty:
                if not finish_signal_queue.empty():
                    # received finish signal
                    return None
                else:
                    raise BufferError
            # got_sgm = False
            # while not got_sgm:
            #     try:
            #         next_segm_chunk = segment_queue.get(block=True, timeout=30)
            #         got_sgm = True
            #     except threadPoolQueue.Empty:  # Queue here refers to the  module, not a class
            #         if num_trials < max_trials:
            #             num_trials += 1
            #         else:
            #             raise BufferError
            #
            if next_segm_chunk is not -1:
                if not finish_signal_queue.empty():
                    # received finish signal
                    return None
                segment_path = next_segm_chunk['name']
                segment_chunk = next_segm_chunk['chunk']
                ibm_cos.put_object(Bucket=output_bucket,
                                   Key=segment_path,
                                   Body=pickle.dumps(segment_chunk))
                print("wrote {}".format(segment_path))
            else:
                return 0

    with ThreadPoolExecutor(3) as pool:
        pool.submit(chunk_reader)
        f2 = pool.submit(chunk_classifier)
        f3 = pool.submit(segment_chunk_writer)
        f3.result()
        total_rows_after, upload_info = f2.result()

    # Metadata upload
    ibm_cos.put_object(Bucket=output_bucket,
                       Key="{}/part_number.pickle".format(my_output_path),
                       Body=pickle.dumps(upload_info))

    finish_signal = _check_end_signal(ibm_cos, args, fname)
    if finish_signal:
        return None

    # Upload manually output path dictionary entry, in speculative it is done
    # inside wait function.
    ibm_cos.put_object(Bucket=output_bucket,
                       Key="{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i),
                       Body=pickle.dumps(my_output_path))

    return total_rows_after

    ########################################################################################################################
    ########################################################################################################################


def _get_row_size(types):
    return np.sum([
        np.dtype(v).itemsize
        for v in types.values()
    ])


def _allocate_dataframe(nms, types, size):
    row_size = _get_row_size(types)
    num_rows = int(size * 1024 ** 2 / row_size)
    rows = {
        nm: np.full(num_rows, 0, dtype=types[nm])
        for nm in nms
    }
    d = DataFrame(rows)

    return d


def _copy_rows(df_buffer, last_row, df_input, i, classified_indexes):
    total_available_rows = len(df_buffer)
    selected_rows_indexes = list(np.where(classified_indexes == i)[0])
    selected_rows_number = len(selected_rows_indexes)

    current_available_rows = total_available_rows - last_row
    surplus_buffer_size = selected_rows_number - current_available_rows
    added_buffer_size = selected_rows_number - surplus_buffer_size
    df_buffer[last_row:(last_row + added_buffer_size)] = df_input.loc[selected_rows_indexes[0:added_buffer_size]]

    additional_buffers = []
    if surplus_buffer_size >= 0:

        new_buffer = df_buffer.copy()
        while surplus_buffer_size >= total_available_rows:
            additional_buffers.append(
                df_input.loc[selected_rows_indexes[added_buffer_size:(added_buffer_size + total_available_rows)]])
            added_buffer_size += total_available_rows
            surplus_buffer_size -= total_available_rows
        new_buffer[0:surplus_buffer_size] = df_input.loc[
            selected_rows_indexes[added_buffer_size:selected_rows_number]]
        last_row = surplus_buffer_size
    else:
        new_buffer = None
        last_row = last_row + selected_rows_number

    return df_buffer, additional_buffers, last_row, new_buffer
