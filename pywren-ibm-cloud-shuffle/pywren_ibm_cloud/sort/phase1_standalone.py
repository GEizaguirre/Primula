import os
import pickle
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue

from ibm_botocore.exceptions import ClientError
from numpy import concatenate, argsort, searchsorted
from numpy.core._multiarray_umath import empty, array
from pandas import DataFrame, read_csv
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.config import DATAFRAME_ALLOCATION_MARGIN, DEFAULT_THREAD_NUM, DEFAULT_GRANULE_SIZE_SHUFFLE, \
    GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION
from pywren_ibm_cloud.sort.utils import _extract_real_bounds_csv, get_normalized_type_names, correct_types, \
    _create_dataframe

def _partition_into_segments_standalone(args, ibm_cos):

    range_i = args['index']
    start_time = time.time()
    times = []

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['parser_info_bucket'],
                                                  Key=args['parser_info_path'])['Body'].read())

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
    #
    try:
        ibm_cos.get_object(Bucket=input_bucket,
                                Key="straggler_{}".format(range_i))
        am_i_straggler = True
        ibm_cos.delete_object(Bucket=input_bucket,
                           Key="straggler_{}".format(range_i))
    except ClientError as ex:
        am_i_straggler = False

    # if am_i_straggler and mode is 0:
    #     time.sleep(sleep_time)
    my_output_path = "{}/{}s".format(intermediate_path, range_i)


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

    q_writes = Queue(len(read_bounds) - 1)
    q_reads = Queue(len(read_bounds) - 1)
    q_finish = Queue()

    for i in range(len(read_bounds) - 1):
        q_reads.put_nowait(i)

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]

    def _chunk_consumer(t_i):

        read_cnks = 0
        my_start_time = time.time()
        key_nm = nms[parser_data['column_numbers'][0]]
        nms_without_key = nms.copy()
        nms_without_key.remove(nms[parser_data['column_numbers'][0]])
        my_row_num = int(parser_data['total_rows_approx'] / num_workers_phase1) + int(
            parser_data['total_rows_approx'] / num_workers_phase1 * DATAFRAME_ALLOCATION_MARGIN)
        normalized_type_names = get_normalized_type_names(types)

        if types[key_nm] not in ['bytes', 'str', 'object']:
            df_columns = {nm: empty(my_row_num, dtype=types[nm]) for nm in nms_without_key}
            key_pointer_str = {
                'key': empty(my_row_num, dtype=types[key_nm]),
                'pointer': empty(my_row_num, dtype='int32')
            }
        else:
            # Cannot allocate data for strings or byte; variable size
            df_columns = {nm: empty(0, dtype=types[nm]) for nm in nms}
            key_pointer_str = {
                'key': empty(0, dtype=types[nms[parser_data['column_numbers'][0]]]),
                'pointer': empty(0, dtype='int32')
            }

        current_lower_bound = 0
        accumulated_size = 0

        while True:

            # Check if task has been signaled to end

            [chunk_name, chunk_size] = q_writes.get(block=True)
            if chunk_name == -1:
                break
            accumulated_size += chunk_size

            # Read intermediate file
            with open(chunk_name, 'rb') as f:
                df = pickle.load(f)

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

            if types[key_nm] not in ['bytes', 'str', 'object']:
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

            sorted_indexes = argsort(key_pointer_str['key'][current_lower_bound:(current_shape + current_lower_bound)],
                                     kind='mergesort')
            sorted_indexes = sorted_indexes.astype('uint32')
            # print("Sorted chunk {}".format(chunk_name))
            key_pointer_str['pointer'][current_lower_bound:(current_shape + current_lower_bound)] = \
                key_pointer_str['pointer'][current_lower_bound:(current_shape + current_lower_bound)][sorted_indexes]
            key_pointer_str['key'][current_lower_bound:(current_shape + current_lower_bound)] = \
                key_pointer_str['key'][current_lower_bound:(current_shape + current_lower_bound)][sorted_indexes]
            del sorted_indexes

            current_lower_bound = current_lower_bound + current_shape

            read_cnks += 1
            # print("Finished chunk {}".format(chunk_name))

            # TODO: Delete this, only for artificial straggler generation.
            # if am_i_straggler:
            #     print("straggler will sleep {} s".format(sleep_time))
            #     time.sleep(sleep_time)
            # if am_i_straggler and mode is 1:
            #     time.sleep(sleep_time)

        medium_time = time.time()
        del df_partial_columns
        del df
        for nm in nms_without_key:
            df_columns[nm] = df_columns[nm][0:current_lower_bound]

        # Global sort
        sorted_indexes = argsort(
            key_pointer_str['key'][0:current_lower_bound],
            kind='mergesort')
        sorted_indexes = sorted_indexes.astype('uint32')

        key_pointer_str['pointer'] = \
            key_pointer_str['pointer'][0:current_lower_bound][
                sorted_indexes]
        key_pointer_str['key'] = \
            key_pointer_str['key'][0:current_lower_bound][sorted_indexes]
        print("Sorted all keys")

        del sorted_indexes

        if parser_data['low_memory']:
            with open("keys", "wb") as f:
                pickle.dump(key_pointer_str['key'], f)
            del key_pointer_str['key']

        for nm in nms_without_key:
            df_columns[nm] = df_columns[nm][key_pointer_str['pointer']]
            # permutate(df_columns[nm], key_pointer_str['pointer'])
        del key_pointer_str['pointer']

        if parser_data['low_memory']:
            with open("keys", "rb") as f:
                key_pointer_str = {'key': pickle.load(f)}

        df = _create_dataframe(df_columns, key_pointer_str['key'], types, key_nm)

        return df, medium_time-my_start_time, time.time()-medium_time

    def _chunk_producer(t_i):

        while not q_reads.empty():

            if q_finish.empty() is not True:
                return None

            c_i = q_reads.get()
            # print("Reading parallel chunk {}".format(c_i))

            # if am_i_straggler:
            #      print("straggler will sleep {} s".format(sleep_time))
            #      time.sleep(sleep_time)

            read_part = ibm_cos.get_object(Bucket=input_bucket,
                                           Key=input_path,
                                           Range=''.join(
                                               ['bytes=', str(read_bounds[c_i]), '-',
                                                str(read_bounds[c_i + 1] - 1)]))

            df = read_csv(read_part['Body'], engine='c', index_col=None, header=None,
                          names=nms,
                          delimiter=delimiter, dtype=types)

            part_size = read_part['ContentLength']

            with open("c_{}".format(c_i), 'wb') as f:
                pickle.dump(df, f)
            q_writes.put(["c_{}".format(c_i), part_size])

        q_writes.put([-1, -1])

    with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
        consumer_future = pool.submit(_chunk_consumer, 0)
        pool.submit(_chunk_producer, 0)

    # If ed is None, function has been signaled to end.
    ed, read_time, sort_time = consumer_future.result()
    times.append(read_time)
    times.append(sort_time)
    if ed is None:
        return None, None

    read_time = time.time()
    print("Chunks read at {}".format(read_time - start_time))

    print("{} shape, types {}".format(ed.shape, ed.dtypes))

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
            segm_end = searchsorted(ed.iloc[:, int(parser_data['column_numbers'][0])],
                                    (right_bound))
        print("segm {} : {} - {}".format(segm_i, segm_start, segm_end))

        # coord_segment = ed[segm_start:segm_end]
        print("{} rows".format(segm_end - segm_start))

        total_rows_after += (segm_end - segm_start)
        # mem_part = coord_segment.memory_usage(index=False)
        # total_bytes_sep += sum([mem_part[j] for j in range(len(coord_segment.columns))])
        segment_data.append([segm_start, segm_end])

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
        s_l = int(granule_info[4][0])
        s_r = int(granule_info[4][1])
        # print("uploading {} rows to segment {}".format(u_b-l_b, "{}/sgm{}/gran{}.pickle".format(my_output_path, s_i, g_n)))
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}/gran{}.pickle".format(my_output_path, s_i, g_n),
                           Body=pickle.dumps(ed[s_l:s_r][l_b:u_b]))
        return len(ed[s_l:s_r][l_b:u_b])

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
        for s_i, s in enumerate(segment_data):
            granule_number = 0
            lower_bound = 0
            upper_bound = rows_per_granule
            shp = s[1] - s[0]
            while upper_bound < shp:
                bounds.append([s_i, lower_bound, upper_bound, granule_number, s])
                lower_bound = upper_bound
                upper_bound += rows_per_granule
                granule_number += 1
            if upper_bound >= shp:
                bounds.append([s_i, lower_bound, shp, granule_number, s])
                granule_number += 1
            upload_info['row_num'].append(shp)
            upload_info['nums'].append(granule_number)

        before_writet = time.time()
        # ThreadPool
        with ThreadPoolExecutor(max_workers=min(len(bounds), PAR_LEVEL_LIMIT_PER_FUNCTION)) as pool:
            uploaded_rows = list(pool.map(_order_upload_segm_info_granuled, bounds))


        after_writet = time.time()
        print("after write time {}".format(after_writet - start_time))

        print("DEV:::MAP PARTITION; {} writes - {} MB written - {} s time".format(len(bounds),
                                                                                  total_bytes_sep / (1024 ** 2),
                                                                                  after_writet - before_writet))

        # Upload output path dictionary entry if first finishing.
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/part_number.pickle".format(my_output_path),
                           Body=pickle.dumps(upload_info))
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i),
                           Body=pickle.dumps(my_output_path))
        print("Write output path into {}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, range_i))

        times.append(time.time()-before_writet)

    return time.time()-start_time

    return total_rows_after, times