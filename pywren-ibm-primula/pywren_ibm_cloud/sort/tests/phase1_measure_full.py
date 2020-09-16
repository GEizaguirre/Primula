import os
import pickle
import re
from concurrent.futures.thread import ThreadPoolExecutor
import time
from random import randrange, seed

from ibm_botocore.exceptions import ClientError
from pandas import read_csv, DataFrame
from queue import Queue as threadPoolQueue
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


def partitions_into_segments_csv(pw, num_workers, parser_info_path, parser_info_bucket, speculative=False):
    print("Map with parallel extraction")

    # Generate arguments with metadata.
    arguments = _create_mapped_args(int(num_workers),
                                    parser_info_path,
                                    parser_info_bucket)

    # Patch correct map function (speculative/not speculative)
    if speculative:
        patch_map_monitor(pw)
        _partition_into_segments = _partition_into_segments_monitored
    else:
        _partition_into_segments = _partition_into_segments_standalone

    # TODO:Remove
    # _partition_into_segments = _partition_into_segments_basic

    print("Mapping {} functions".format(int(num_workers)))

    ft = pw.map(_partition_into_segments, arguments)
    res = pw.get_result(ft)

    if speculative:
        unpatch_map_monitor(pw)
    print([ v[0] for v in res])

    kind = "tim"
    numr = np.array([v[0] for v in res])
    fmean, std = numr.mean(), numr.std()
    print(f'    {kind}: {fmean}±{std} float64 per worker, {len(res)} workers')
    times = np.array([v[1] for v in res]) * 1e6
    fmean, std = times.mean(), times.std()
    print(f'    {kind}: {fmean:.3f}±{std:.3f} us per loop')

def _create_mapped_args(num_maps, parser_info_path, parser_info_bucket):
    arguments = [
        {'args': {
            'index': i,
            'parser_info_path': parser_info_path,
            'parser_info_bucket': parser_info_bucket
        }}
        for i in range(num_maps)
    ]
    return arguments


########################################################################################################################
# No monitor option
# _partition_into_segments_standalone
def _partition_into_segments_standalone(args, ibm_cos):
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

    # Adapt
    my_output_path = "{}/{}".format(intermediate_path, range_i)

    partition_ranges = pickle.loads(ibm_cos.get_object(Bucket=chunk_range_bucket,
                                                       Key=chunk_range_file_path)['Body'].read())
    print("partition ranges {}".format(partition_ranges))

    total_size = parser_data['total_size']
    delimiter = parser_data['delimiter']
    lower_bound = partition_ranges[range_i]
    upper_bound = partition_ranges[range_i + 1]
    lower_bound, upper_bound = _extract_real_bounds_csv(ibm_cos, lower_bound, upper_bound, total_size,
                                                        input_bucket,
                                                        input_path)

    print("started classification of chunk {} ({} to {} - {})".format(range_i,
                                                                      lower_bound,
                                                                      upper_bound,
                                                                      upper_bound - lower_bound))
    print("{} threads".format(DEFAULT_THREAD_NUM))
    # Read partition file from COS
    segment_info = pickle.loads(ibm_cos.get_object(Bucket=partition_bucket,
                                                   Key=partition_file_path)['Body'].read())
    print("Got segment info from COS *********************")
    print("{}".format(segment_info))
    # Granuled upload
    total_bytes = upper_bound - lower_bound
    print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))
    chunk_size = int(granule_size * (1024 ** 2))
    read_bounds = list(range(lower_bound, upper_bound, chunk_size))
    read_bounds.append(upper_bound)
    for i in range(len(read_bounds) - 1):
        lb, ub = _extract_real_bounds_csv(ibm_cos, read_bounds[i], read_bounds[i + 1], total_size, input_bucket,
                                          input_path)
        read_bounds[i] = lb
    print("Reading in {} chunks".format(len(read_bounds) - 1))
    print(read_bounds)

    sizel = []
    before_readt = time.time()

    q_writes = threadPoolQueue(len(read_bounds) - 1)
    q_reads = threadPoolQueue(len(read_bounds) - 1)
    #q_start = threadPoolQueue(1)

    for i in range(len(read_bounds) - 1):
        q_reads.put_nowait(i)

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]

    print(parser_data)
    print(types)
    print(nms)

    # seed()
    # straggler_iteration = randrange(DEFAULT_THREAD_NUM - 1)
    print("reading from bucket {} and file {}".format(input_bucket, input_path))

    def _chunk_consumer(t_i):

        read_cnks = 0
        nms_without_key = nms.copy()
        nms_without_key.remove(nms[parser_data['column_numbers'][0]])
        my_row_num = int(parser_data['total_rows_approx'] / num_workers_phase1) + int(
                        parser_data['total_rows_approx'] / num_workers_phase1 * DATAFRAME_ALLOCATION_MARGIN)
        normalized_type_names = get_normalized_type_names(types)

        # signal immediate start
        #q_start.put(1)
        print("Preallocating numpy arrays of length {}".format(my_row_num))
        if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
            df_columns = {nm: empty(my_row_num, dtype=types[nm]) for nm in nms_without_key}
            key_pointer_str = {
                'key': empty(my_row_num, dtype=types[nms[parser_data['column_numbers'][0]]]),
                'pointer': empty(my_row_num, dtype='uint32')
            }
        else:
            # Cannot allocate data for strings or byte; variable size
            df_columns = {nm: empty(0, dtype=types[nm])
                          for nm in nms}
            key_pointer_str = {
                'key': empty(0, dtype=types[nms[parser_data['column_numbers'][0]]]),
                'pointer': empty(0, dtype='uint32')
            }

        print("Allocated array info at start")
        print("df_columns {}".format([v.dtype for v in df_columns.values() ]))
        print("keys {}".format(key_pointer_str['key'].dtype))
        # return None

        current_lower_bound = 0

        while True:



            # TODO: remove, only for straggler testing.
            # if read_cnks == straggler_iteration:
            #     try:
            #         ibm_cos.get_object(Bucket=dataset_bucket,
            #                            Key="straggler_call")
            #
            #     except ClientError as ex:
            #         if ex.response['Error']['Code'] == 'NoSuchKey':
            #             ibm_cos.put_object(Bucket=dataset_bucket,
            #                                Key="straggler_call", Body=pickle.dumps(range_i))
            #             ibm_cos.put_object(Bucket=dataset_bucket,
            #                                Key="straggler_id_{}".format(range_i), Body=pickle.dumps(range_i))
            #             i_am_speculative = True
            #         else:
            #             raise

            chunk_name = q_writes.get(block=True)
            print("Processing chunk {}".format(chunk_name))

            with open(chunk_name, 'rb') as f:
                df = pickle.load(f)
            os.remove(chunk_name)
            # Adjust types
            if not correct_types(df, normalized_type_names):
                df_partial_columns = {nm: array( list(df[nm]), dtype=types[nm]) for nm in nms}
                del df
                nm = nms[0]
                df = DataFrame(df_partial_columns[nm])
                for nm in nms[1:]:
                    df[nm] = df_partial_columns[nm]
                df.columns = nms
            current_shape = df.shape[0]

            # Key-pointer sort
            if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                key_pointer_str['key'][current_lower_bound:(current_lower_bound + current_shape)] = \
                    df[str(parser_data['column_numbers'][0])]

                key_pointer_str['pointer'][current_lower_bound:(current_lower_bound + current_shape)] = \
                    array(range(current_lower_bound, current_shape + current_lower_bound))

                for nm in nms_without_key:
                    df_columns[nm][current_lower_bound:(current_lower_bound + current_shape)] = df[nm]
            else:
                key_pointer_str['key'] = \
                    concatenate([key_pointer_str['key'], df[str(parser_data['column_numbers'][0])]])
                key_pointer_str['pointer'] = \
                    concatenate([key_pointer_str['pointer'],
                                    array(range(current_lower_bound, current_shape + current_lower_bound))])
                for nm in nms_without_key:
                    df_columns[nm] = concatenate([df_columns[nm], df[nm]])


            current_lower_bound = current_lower_bound + current_shape

            read_cnks += 1

            # TODO: Delete this, only for artificial straggler generation.
            # if i_am_speculative:
            #     time.sleep(parser_data['sleep_time'])

            print("Finished chunk {}".format(chunk_name))
            if read_cnks == len(read_bounds) - 1:

                sorted_indexes = argsort(key_pointer_str['key'][0:(current_lower_bound)],
                                         kind='mergesort')
                sorted_indexes = sorted_indexes.astype('uint32')
                print("Sorted chunk {}".format(chunk_name))
                key_pointer_str['pointer'][0:(current_lower_bound)] = \
                    key_pointer_str['pointer'][0:(current_lower_bound)][sorted_indexes]
                key_pointer_str['key'][0:(current_lower_bound)] = \
                    key_pointer_str['key'][0:(current_lower_bound)][sorted_indexes]
                del sorted_indexes



                #df = DataFrame()
                # print("Acotating")
                # key_pointer_str['key'] = key_pointer_str['key'][0:current_lower_bound]
                # # We save keys in auxiliary file for memory use optimization.
                # with open("aux_f", 'wb') as f:
                #     pickle.dump(key_pointer_str['key'], f)
                # key_pointer_str['key'] = empty(0, dtype=types[nms[parser_data['column_numbers'][0]]])
                # del key_pointer_str['key']
                # for nm in nms_without_key:
                #     df_columns[nm] = df_columns[nm][0:current_lower_bound]
                # key_pointer_str['pointer'] = key_pointer_str['pointer'][0:current_lower_bound]
                # print("Permutating")
                # for nm in nms_without_key:
                #     df_columns[nm].take(key_pointer_str['pointer'], axis=0, out=df_columns[nm], mode="clip")
                #     print(df_columns[nm])
                #     print("Permutated column {}".format(nm))
                # print("Recovering keys")
                # with open("aux_f", 'rb') as f:
                #     key_pointer_str['key'] = pickle.load(f)
                # key_pointer_str['pointer'] = empty(0, dtype=types[nms[parser_data['column_numbers'][0]]])
                # del key_pointer_str['pointer']
                # print("Generating dataframe")
                # sorted_names = sort_names_by_type_size(types, nms)
                # if sorted_names[0] is nms[parser_data['column_numbers'][0]]:
                #     # DataFrame intialization array does not generate a copy.
                #     df = DataFrame(key_pointer_str['key'])
                # else:
                #     df = DataFrame(df_columns[sorted_names[0]])
                # df.columns = [sorted_names[0]]
                # print("column {} to frame".format(nm))
                # for nm in sorted_names[1:]:
                #     if nm is nms[parser_data['column_numbers'][0]]:
                #         df[nm] = key_pointer_str['key']
                #     else:
                #         df[nm] = df_columns[nm]
                #     print("column {} to frame".format(nm))

                if parser_data['column_numbers'][0] is 0:
                    df = DataFrame(key_pointer_str['key'][0:current_lower_bound])
                    del key_pointer_str['key']
                else:
                    df = DataFrame(df_columns[nms[0]][0:current_lower_bound]
                                   [key_pointer_str['pointer'][0:current_lower_bound]])
                    del (df_columns[nms[0]])
                df.columns = [nms[0]]
                for nm in nms[1:]:
                    if nms[parser_data['column_numbers'][0]] is nm:
                        df[nm] = key_pointer_str['key'][0:current_lower_bound]
                        del key_pointer_str['key']
                    else:
                        df[nm] = df_columns[nm][0:current_lower_bound][
                            key_pointer_str['pointer'][0:current_lower_bound]]
                        del (df_columns[nm])
                del (key_pointer_str)

                return df[nms]

    def _chunk_producer(t_i):
        #q_start.get(block=True, timeout=40)
        while not q_reads.empty():
            c_i = q_reads.get()
            print("Reading parallel chunk {}".format(c_i))

            read_part = ibm_cos.get_object(Bucket=input_bucket,
                                           Key=input_path,
                                           Range=''.join(
                                               ['bytes=', str(read_bounds[c_i]), '-',
                                                str(read_bounds[c_i + 1] - 1)]))

            df = read_csv(read_part['Body'], engine='c', index_col=None, header=None,
                          names=nms,
                          delimiter=delimiter, dtype=types)


            part_size = read_part['ContentLength']
            sizel.insert(c_i, part_size)

            with open("c_{}".format(c_i), 'wb') as f:
                pickle.dump(df, f)
                # f.write(read_part['Body'].read())
            print("Read parallel chunk {} of size {} MB".format(c_i, part_size / (1024 ** 2)))
            q_writes.put_nowait("c_{}".format(c_i))

    time1 = time.time()
    with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
        consumer_future = pool.submit(_chunk_consumer, 0)
        pool.map(_chunk_producer, range(DEFAULT_THREAD_NUM - 1))


    after_readt = time.time()
    print("DEV:::MAP PARTITION; {} reads - {} MB read - {} s time".format(len(sizel), sum(sizel) / (1024 ** 2),
                                                                          after_readt - before_readt))

    ed  = consumer_future.result()

    data_subcollection_time = time.time()

    print("{} shape, types {}".format(ed.shape, ed.dtypes))

    # Pd
    prev_num_rows = ed.shape[0]
    mem = ed.memory_usage(index=False)
    print(mem)
    print(prev_num_rows)

    bytes_per_row = sum([mem[i] / prev_num_rows for i in range(len(ed.columns))])

    data_sorted_time = time.time()
    print("Data sorted at {}".format(data_sorted_time - start_time))

    # print("Extracting segments")
    total_rows_after = 0
    segment_data = list()
    total_bytes_sep = 0

    print("rows to upload: {}".format(ed.shape[0]))
    segm_start = 0
    for segm_i in range(1, len(segment_info) + 1):
        # print("Coordinate {} segm {}".format(i, segm_i))

        if segm_i == len(segment_info):
            segm_end = ed.shape[0]
            # segm_start, segm_end = searchsorted(ed.iloc[:, int(parser_data['column_numbers'][0])],
            #                           (left_bound, right_bound))
        else:
            right_bound = segment_info[segm_i]
            segm_end = searchsorted(ed.iloc[:, int(parser_data['column_numbers'][0])],
                                    (right_bound))
            # segm_start, segm_end = searchsorted(ed.iloc[:, int(parser_data['column_numbers'][0])],
            #                                     (left_bound, right_bound))

        # print("left_bound {}, right_bound {}".format(left_bound, right_bound))

        # Np
        # segm_start, segm_end = searchsorted(ed[:, 1], (left_bound, right_bound))

        # Pd

        print("segm {} : {} - {}".format(segm_i, segm_start, segm_end))

        coord_segment = ed[segm_start:segm_end]


        print("{} rows".format(coord_segment.shape[0]))

        total_rows_after += coord_segment.shape[0]
        mem_part = coord_segment.memory_usage(index=False)
        total_bytes_sep += sum([mem_part[j] for j in range(len(coord_segment.columns))])
        segment_data.append(coord_segment)

        segm_start = segm_end

    segment_extraction_time = time.time()
    print("Data extracted at {}".format(segment_extraction_time - start_time))

    # print("Uploading segments ({} total rows)".format(total_rows))

    def _order_upload_segm_info(segm_i):

        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}.pickle".format(my_output_path, segm_i),
                           Body=pickle.dumps(segment_data[segm_i]))

    def _order_upload_segm_info_granuled(granule_info):
        s_i = int(granule_info[0])
        l_b = int(granule_info[1])
        u_b = int(granule_info[2])
        g_n = int(granule_info[3])
        print("uploading {} rows to segment {}".format(segment_data[s_i][l_b:u_b].shape[0], s_i))
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}/gran{}.pickle".format(my_output_path, s_i, g_n),
                           Body=pickle.dumps(segment_data[s_i][l_b:u_b]))
        return segment_data[s_i][l_b:u_b].shape[0]

    if DEFAULT_GRANULE_SIZE_SHUFFLE == -1:
        with ThreadPoolExecutor(max_workers=min(len(segment_info), 128)) as pool:
            pool.map(_order_upload_segm_info, range(len(segment_info) - 1))
        print("granule -1 time {}".format(time.time() - start_time))
    # Granuled upload
    else:

        # Calculate lines per granule
        rows_per_granule = (granule_size * GRANULE_SIZE_TO_BYTES) // bytes_per_row
        bounds = []
        # Generate bounds per segment
        upload_info = {'row_num': [], 'nums': []}
        for s in range(len(segment_data)):
            row_counter = 0
            granule_number = 0
            lower_bound = 0
            upper_bound = rows_per_granule
            while upper_bound < segment_data[s].shape[0]:
                bounds.append([s, lower_bound, upper_bound, granule_number])
                lower_bound = upper_bound
                upper_bound += rows_per_granule
                granule_number += 1
            if upper_bound >= segment_data[s].shape[0]:
                bounds.append([s, lower_bound, segment_data[s].shape[0], granule_number])
                granule_number += 1

            upload_info['row_num'].append(segment_data[s].shape[0])
            upload_info['nums'].append(granule_number)

        print("part number parallel upload start time {}".format(time.time() - start_time))

        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/part_number.pickle".format(my_output_path),
                           Body=pickle.dumps(upload_info))

        print("part number uploaded time {}".format(time.time() - start_time))

        before_writet = time.time()
        print("before write time {}".format(before_writet - start_time))

        # ThreadPool
        print("uploading {} rows".format(total_rows_after))
        with ThreadPoolExecutor(max_workers=min(len(bounds), PAR_LEVEL_LIMIT_PER_FUNCTION)) as pool:
            uploaded_rows = list(pool.map(_order_upload_segm_info_granuled, bounds))

        after_writet = time.time()
        print("after write time {}".format(after_writet - start_time))

        print("DEV:::MAP PARTITION; {} writes - {} MB written - {} s time".format(len(bounds),
                                                                                  total_bytes_sep / (1024 ** 2),
                                                                                  after_writet - before_writet))

        data_upload_time = time.time()
        print("Data uploaded at {}".format(data_upload_time - start_time))

        # Upload manually output path dictionary entry, in speculative it is done
        # inside wait function.
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i),
                           Body=pickle.dumps(my_output_path))

    return [ total_rows_after, data_subcollection_time-time1 ]


########################################################################################################################
########################################################################################################################

# Monitor option
# _partition_into_segments_monitored
def _partition_into_segments_monitored(args, ibm_cos):

    range_i = args['arguments']['args']['index']
    start_time = time.time()

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['arguments']['args']['parser_info_bucket'],
                                                  Key=args['arguments']['args']['parser_info_path'])['Body'].read())

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

    # sleep_time = args['arguments']['args']['sleep_time']
    # mode = args['arguments']['args']['mode']
    sleep_time = 10

    # try:
    #     ibm_cos.get_object(Bucket=input_bucket,
    #                             Key="straggler_{}".format(range_i))
    #     am_i_straggler = True
    #     ibm_cos.delete_object(Bucket=input_bucket,
    #                        Key="straggler_{}".format(range_i))
    # except ClientError as ex:
    #     am_i_straggler = False

    # if am_i_straggler and mode is 0:
    #     time.sleep(sleep_time)

    # Protocol to know if i am speculative.
    fname, is_speculative = _check_if_speculative(ibm_cos, args, int(range_i))
    if is_speculative:
        my_output_path = "{}/{}s".format(intermediate_path, range_i)
    else:
        my_output_path = "{}/{}".format(intermediate_path, range_i)

    # print("straggler: {} - speculative: {}".format(am_i_straggler, is_speculative))

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
    segment_info = pickle.loads(ibm_cos.get_object(Bucket=partition_bucket,
                                                   Key=partition_file_path)['Body'].read())
    print("Got segment info from COS *********************")

    # Granuled upload
    total_bytes = upper_bound - lower_bound

    chunk_size = int(granule_size * (1024 ** 2))
    read_bounds = list(range(lower_bound, upper_bound, chunk_size))
    read_bounds.append(upper_bound)
    for i in range(len(read_bounds) - 1):
        lb, ub = _extract_real_bounds_csv(ibm_cos, read_bounds[i], read_bounds[i + 1], total_size, input_bucket,
                                          input_path)
        read_bounds[i] = lb

    sizel = []
    before_readt = time.time()

    q_writes = threadPoolQueue(len(read_bounds) - 1)
    q_reads = threadPoolQueue(len(read_bounds) - 1)

    for i in range(len(read_bounds) - 1):
        q_reads.put_nowait(i)

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]

    finish_signal = False

    def _chunk_consumer(t_i):

        # if am_i_straggler:
        #    print("straggler will sleep {} s".format(sleep_time))

        read_cnks = 0
        # i_am_speculative = False
        # For calculation of each sort time.
        # sort_times = []
        nms_without_key = nms.copy()
        nms_without_key.remove(nms[parser_data['column_numbers'][0]])

        if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
            df_columns = {nm: empty(
                int(parser_data['total_rows_approx'] / num_workers_phase1) + int(
                    parser_data['total_rows_approx'] / num_workers_phase1 * DATAFRAME_ALLOCATION_MARGIN),
                dtype=types[nm])
                for nm in nms_without_key}
            key_pointer_str = {
                'key': empty(int(parser_data['total_rows_approx'] / num_workers_phase1) + int(
                    parser_data['total_rows_approx'] / num_workers_phase1 * DATAFRAME_ALLOCATION_MARGIN),
                             dtype=types[nms[parser_data['column_numbers'][0]]]),
                'pointer': empty(int(parser_data['total_rows_approx'] / num_workers_phase1) + int(
                    parser_data['total_rows_approx'] / num_workers_phase1 * DATAFRAME_ALLOCATION_MARGIN), dtype='int32')
            }
        else:
            # Cannot allocate data for strings or byte; variable size
            df_columns = {nm: empty(0, dtype=types[nm])
                          for nm in nms}
            key_pointer_str = {
                'key': empty(0, dtype=types[nms[parser_data['column_numbers'][0]]]),
                'pointer': empty(0, dtype='int32')
            }

        current_lower_bound = 0
        accumulated_size = 0
            #
            #     except ClientError as ex:
        while True:

            # TODO: Delete this, only for artificial straggler generation
            # if read_cnks == straggler_iteration:
            #     try:
            #         ibm_cos.get_object(Bucket=dataset_bucket,
            #                            Key="straggler
            #         if ex.response['Error']['Code'] == 'NoSuchKey':
            #             ibm_cos.put_object(Bucket=dataset_bucket,
            #                                Key="straggler_call", Body=pickle.dumps(range_i))
            #             ibm_cos.put_object(Bucket=dataset_bucket,
            #                                Key="straggler_id_{}".format(range_i), Body=pickle.dumps(range_i))
            #             i_am_speculative = True
            #         else:
            #             raise

            # Check if task has been signaled to end
            finish_signal = _check_end_signal(ibm_cos, args, fname)
            if finish_signal:
                return None

            [chunk_name, chunk_size] = q_writes.get(block=True)
            accumulated_size += chunk_size

            print("Processing chunk {}".format(chunk_name))

            # Read intermediate file
            with open(chunk_name, 'rb') as f:
                df = pickle.load(f)

            # Remove intermediate file
            os.remove(chunk_name)

            current_shape = df.shape[0]

            if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                key_pointer_str['key'][current_lower_bound:(current_lower_bound + current_shape)] = \
                    df[str(parser_data['column_numbers'][0])]

                key_pointer_str['pointer'][current_lower_bound:(current_lower_bound + current_shape)] = \
                    array(range(current_lower_bound, current_shape + current_lower_bound))

                for nm in nms_without_key:
                    df_columns[nm][current_lower_bound:(current_lower_bound + current_shape)] = df[nm]
            else:
                key_pointer_str['key'] = \
                    concatenate([key_pointer_str['key'], df[str(parser_data['column_numbers'][0])]])
                key_pointer_str['pointer'] = \
                    concatenate([key_pointer_str['pointer'],
                                 array(range(current_lower_bound, current_shape + current_lower_bound))])
                for nm in nms_without_key:
                    df_columns[nm] = concatenate([df_columns[nm], df[nm]])

            sorted_indexes = argsort(
                key_pointer_str['key'][
                0:(current_lower_bound + current_shape)],
                kind='mergesort')
            # print("Sorted chunk {}".format(chunk_name))

            key_pointer_str['pointer'][0:(current_lower_bound + current_shape)] = \
                key_pointer_str['pointer'][0:(current_lower_bound + current_shape)][sorted_indexes]

            key_pointer_str['key'][0:(current_lower_bound + current_shape)] = \
                key_pointer_str['key'][0:(current_lower_bound + current_shape)][sorted_indexes]

            current_lower_bound = current_lower_bound + current_shape

            read_cnks += 1
            print("Finished chunk {}".format(chunk_name))

            if not is_speculative:
                _upload_progress(ibm_cos, range_i, accumulated_size / total_bytes, time.time() - start_time, args)

            # TODO: Delete this, only for artificial straggler generation.
            # if am_i_straggler:
            #     print("straggler will sleep {} s".format(sleep_time))
            #     time.sleep(sleep_time)
            # if am_i_straggler and mode is 1:
            #     time.sleep(sleep_time)

            # Check if task has been signaled to end
            finish_signal = _check_end_signal(ibm_cos, args, fname)
            if finish_signal:
                return None

            if read_cnks == len(read_bounds) - 1:

                # for nm in nms_without_key:
                #     df_columns[nm] = \
                #         df_columns[nm][0:current_lower_bound][
                #             key_pointer_str['pointer'][0:current_lower_bound]]
                # df_columns[nms[parser_data['column_numbers'][0]]] = key_pointer_str['key'][0:current_lower_bound]
                # df = DataFrame(df_columns)
                if parser_data['column_numbers'][0] is 0:
                    df = DataFrame(key_pointer_str['key'][0:current_lower_bound])
                    del key_pointer_str['key']
                else:
                    df = DataFrame(df_columns[nms[0]][0:current_lower_bound]
                                   [key_pointer_str['pointer'][0:current_lower_bound]])
                    del (df_columns[nms[0]])
                df.columns = [nms[0]]
                for nm in nms[1:]:
                    if nms[parser_data['column_numbers'][0]] is nm:
                        df[nm] = key_pointer_str['key'][0:current_lower_bound]
                        del key_pointer_str['key']
                    else:
                        df[nm] = df_columns[nm][0:current_lower_bound][
                            key_pointer_str['pointer'][0:current_lower_bound]]
                        del (df_columns[nm])
                del (key_pointer_str)
                # return df, sort_times
                return df

    def _chunk_producer(t_i):
        while not q_reads.empty():

            if finish_signal is True:
                return None

            c_i = q_reads.get()
            print("Reading parallel chunk {}".format(c_i))

            read_part = ibm_cos.get_object(Bucket=input_bucket,
                                           Key=input_path,
                                           Range=''.join(
                                               ['bytes=', str(read_bounds[c_i]), '-',
                                                str(read_bounds[c_i + 1] - 1)]))

            df = read_csv(read_part['Body'], engine='c', index_col=None, header=None,
                          names=nms,
                          delimiter=delimiter, dtype=types)

            part_size = read_part['ContentLength']
            sizel.insert(c_i, part_size)

            with open("c_{}".format(c_i), 'wb') as f:
                pickle.dump(df, f)
                # f.write(read_part['Body'].read())
            print("Read parallel chunk {} of size {} MB".format(c_i, part_size / (1024 ** 2)))
            q_writes.put_nowait(["c_{}".format(c_i), part_size])

    with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
        consumer_future = pool.submit(_chunk_consumer, 0)
        pool.map(_chunk_producer, range(DEFAULT_THREAD_NUM - 1))

    read_time = time.time()
    print("Chunks read at {}".format(read_time - start_time))

    after_readt = time.time()
    print("DEV:::MAP PARTITION; {} reads - {} MB read - {} s time".format(len(sizel), sum(sizel) / (1024 ** 2),
                                                                          after_readt - before_readt))

    ed = consumer_future.result()
    print(ed)

    # If ed is None, function has been signaled to end.
    if ed is None:
        return None

    data_subcollection_time = time.time()

    print("{} shape, types {}".format(ed.shape, ed.dtypes))

    # print("TYPES: {}".format(ed.dtypes))

    data_collection_time = time.time()
    print("Chunks concat at {}".format(data_subcollection_time - start_time))
    # print("Data collected")

    # Pd
    prev_num_rows = ed.shape[0]
    mem = ed.memory_usage(index=False)

    bytes_per_row = sum([mem[i] / prev_num_rows for i in range(len(ed.columns))])

    data_sorted_time = time.time()
    print("Data sorted at {}".format(data_sorted_time - start_time))

    # print("Extracting segments")
    total_rows_after = 0
    segment_data = list()
    total_bytes_sep = 0

    print("rows to upload: {}".format(ed.shape[0]))
    print("segment info: ")
    print(segment_info)
    segm_start = 0
    for segm_i in range(1, len(segment_info) + 1):

        if segm_i == len(segment_info):
            segm_end = ed.shape[0]

        else:
            right_bound = segment_info[segm_i]
            print("right bound {}".format(right_bound))
            print("searching on column {}".format(nms[parser_data['column_numbers'][0]]))
            segm_end = searchsorted(ed.iloc[:, int(parser_data['column_numbers'][0])],
                                    (right_bound))

        coord_segment = ed[segm_start:segm_end]

        total_rows_after += coord_segment.shape[0]
        mem_part = coord_segment.memory_usage(index=False)
        total_bytes_sep += sum([mem_part[j] for j in range(len(coord_segment.columns))])
        segment_data.append(coord_segment)

        segm_start = segm_end

    def _order_upload_segm_info(segm_i):

        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}.pickle".format(my_output_path, segm_i),
                           Body=pickle.dumps(segment_data[segm_i]))

    def _order_upload_segm_info_granuled(granule_info):
        s_i = int(granule_info[0])
        l_b = int(granule_info[1])
        u_b = int(granule_info[2])
        g_n = int(granule_info[3])

        print("Uploaded {} rows for segment {}".format(u_b-l_b, s_i))

        # Pd
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}/gran{}.pickle".format(my_output_path, s_i, g_n),
                           Body=pickle.dumps(segment_data[s_i][l_b:u_b]))

        return segment_data[s_i][l_b:u_b].shape[0]

    if DEFAULT_GRANULE_SIZE_SHUFFLE == -1:
        with ThreadPoolExecutor(max_workers=min(len(segment_info), 128)) as pool:
            pool.map(_order_upload_segm_info, range(len(segment_info) - 1))

    # Granuled upload
    else:

        # Calculate lines per granule
        rows_per_granule = (granule_size * GRANULE_SIZE_TO_BYTES) // bytes_per_row
        bounds = []
        # Generate bounds per segment
        upload_info = {'row_num': [], 'nums': []}
        for s in range(len(segment_data)):
            row_counter = 0
            granule_number = 0
            lower_bound = 0
            upper_bound = rows_per_granule
            while upper_bound < segment_data[s].shape[0]:
                bounds.append([s, lower_bound, upper_bound, granule_number])
                lower_bound = upper_bound
                upper_bound += rows_per_granule
                granule_number += 1
            if upper_bound >= segment_data[s].shape[0]:
                bounds.append([s, lower_bound, segment_data[s].shape[0], granule_number])
                granule_number += 1

            upload_info['row_num'].append(segment_data[s].shape[0])
            upload_info['nums'].append(granule_number)

        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/part_number.pickle".format(my_output_path),
                           Body=pickle.dumps(upload_info))

        before_writet = time.time()

        # ThreadPool
        with ThreadPoolExecutor(max_workers=min(len(bounds), PAR_LEVEL_LIMIT_PER_FUNCTION)) as pool:
            uploaded_rows = list(pool.map(_order_upload_segm_info_granuled, bounds))

        after_writet = time.time()
        print("after write time {}".format(after_writet - start_time))

        print("DEV:::MAP PARTITION; {} writes - {} MB written - {} s time".format(len(bounds),
                                                                                  total_bytes_sep / (1024 ** 2),
                                                                                  after_writet - before_writet))

        data_upload_time = time.time()
        print("Data uploaded at {}".format(data_upload_time - start_time))

        # Upload output path dictionary entry if first finishing.
        if not _check_end_signal(ibm_cos, args, fname):
            ibm_cos.put_object(Bucket=output_bucket,
                               Key="{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i),
                               Body=pickle.dumps(my_output_path))
            print("Write output path into {}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i))

    return total_rows_after

########################################################################################################################
########################################################################################################################