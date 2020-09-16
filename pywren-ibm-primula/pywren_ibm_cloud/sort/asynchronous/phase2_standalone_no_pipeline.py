from queue import Queue as threadPoolQueue
import time
from queue import Empty
from concurrent.futures.thread import ThreadPoolExecutor
import pickle
from numpy import empty, array, argsort, concatenate, append
from pandas import DataFrame
from ibm_botocore.exceptions import ClientError
from time import sleep

from pywren_ibm_cloud.sort.asynchronous.config import MAX_TRIALS_PER_PARTITION, \
    SLEEP_TIME_ASYNC, MAX_READ_TIME, MAXIMUM_RECONNECTIONS
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _check_end_signal, \
    _upload_progress, _check_if_speculative
from pywren_ibm_cloud.sort.config import GRANULE_SIZE_TO_BYTES, MIN_MULTIPART_SIZE, \
    DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN, PHASE_2_MULTIPLIER, OUTPUT_GRANULARITY, MAX_RETRIES, \
    DEFAULT_MULTIPART_NUMBER
from pywren_ibm_cloud.sort.utils import _create_dataframe, _multipart_upload


def _reduce_partitions(args, ibm_cos):
    print("new reducer")
    print(args)

    times = []

    segms = args['segms']
    print("Assigned segments {}".format(segms))

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['parser_info_bucket'],
                                                  Key=args['parser_info_path'])['Body'].read())

    input_bucket = parser_data['input_bucket']
    output_bucket = parser_data['output_bucket']
    input_path = parser_data['input_path']
    intermediate_path = parser_data['intermediate_path']
    output_path = parser_data['output_path']
    granule_size = parser_data['granularity']
    partition_bucket = parser_data['partition_bucket']
    partition_file_path = parser_data['partition_file_path']
    chunk_range_bucket = parser_data['chunk_range_bucket']
    chunk_range_file_path = parser_data['chunk_range_file_path']
    partition_number = parser_data['num_workers_phase1']

    my_output_path = output_path

    sleep_time = 4
    try:
        ibm_cos.get_object(Bucket=input_bucket,
                           Key="straggler_p2_{}".format(segms[0]))
        am_i_straggler = True
        ibm_cos.delete_object(Bucket=input_bucket,
                              Key="straggler_p2_{}".format(segms[0]))
    except ClientError as ex:
        am_i_straggler = False

    # print("straggler: {} - speculative: {}".format(am_i_straggler, is_speculative))

    for segm_i in segms:

        print("Reducing segment {} from {} partition".format(segm_i, partition_number))

        # thread pool
        q_writes = threadPoolQueue()
        q_finish = threadPoolQueue()

        types = dict()
        for i in range(len(parser_data['dtypes'])):
            types[str(i)] = parser_data['dtypes'][i]
        nms = [str(i) for i in range(len(parser_data['dtypes']))]

        before_readt = time.time()

        # Generate output path dictionary
        def _chunk_producer(t_i):

            print("started chunk producer")

            before_readt = time.time()
            pendent_partition_i = list(range(partition_number))
            partition_pos = segm_i % partition_number
            while len(pendent_partition_i) != 0:

                p_i = pendent_partition_i[partition_pos]

                try:
                    output_path = "{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, p_i)
                    cos_read_path = ibm_cos.get_object(Bucket=output_bucket, Key=output_path)['Body'].read()
                    next_path = pickle.loads(cos_read_path)
                    part_num_name = "{}/part_number.pickle".format(next_path)
                    cos_read_parts = ibm_cos.get_object(Bucket=output_bucket, Key=part_num_name)['Body'].read()
                    parts = pickle.loads(cos_read_parts)

                    for c_i in range(parts['nums'][int(segm_i)]):

                        if not q_finish.empty():
                            q_writes.put(-2)
                            return None

                        read_part = ibm_cos.get_object(Bucket=output_bucket,
                                                       Key="{}/sgm{}/gran{}.pickle".format(next_path, segm_i, c_i))
                        if am_i_straggler:
                            print("straggler will sleep {} s".format(sleep_time))
                            time.sleep(sleep_time)

                        with open("p{}_c{}".format(p_i, c_i), 'wb') as f:
                            f.write(read_part['Body'].read())

                        q_writes.put("p{}_c{}".format(p_i, c_i))

                    pendent_partition_i.remove(p_i)

                except ClientError as ex:
                    if ex.response['Error']['Code'] == 'NoSuchKey':
                        if time.time()-before_readt > MAX_READ_TIME:
                            q_writes.put(-2)
                            print("Could not read partition {}".format(p_i))
                            return None
                        # Mapper has still not finished
                        time.sleep(SLEEP_TIME_ASYNC/len(pendent_partition_i))
                except:
                    raise

                # Get next partition
                if len(pendent_partition_i) != 0:
                    partition_pos = (partition_pos + 1) % len(pendent_partition_i)

            q_writes.put(-1)
            return None


        def _chunk_consumer():

            my_start_time = time.time()

            print("started chunk consumer")

            my_allocation_margin = DATAFRAME_ALLOCATION_MARGIN * PHASE_2_MULTIPLIER
            # my_allocation_margin = DATAFRAME_ALLOCATION_MARGI

            read_cnks = 0
            key_nm = nms[parser_data['column_numbers'][0]]
            start_time = time.time()

            approximate_rows = int(parser_data['total_rows_approx'] / parser_data['num_workers_phase2']) + (int(
                parser_data['total_rows_approx'] / parser_data['num_workers_phase2'] * my_allocation_margin))

            if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                df_columns = {
                    nm: empty(approximate_rows, dtype=types[nm]) for nm in nms
                }
                key_pointer_str = {
                    'key': empty(approximate_rows, dtype=types[nms[parser_data['column_numbers'][0]]]),
                    'pointer': empty(approximate_rows, dtype='int32')
                }
            else:
                df_columns = {nm: empty(0, dtype=types[nm])
                              for nm in nms}
                key_pointer_str = {
                    'key': empty(0, dtype=types[nms[parser_data['column_numbers'][0]]]),
                    'pointer': empty(0, dtype='int32')
                }

            nms_without_key = nms.copy()
            nms_without_key.remove(nms[parser_data['column_numbers'][0]])

            current_lower_bound = 0

            while True:

                num_rec = 0
                while True:
                    try:
                        chunk_name = q_writes.get(block=True, timeout=20)
                        if chunk_name == -2:
                            return None, None, None, None
                        break
                    except TimeoutError:
                        print("reconnect")
                        num_rec +=1
                    except Empty:
                        print("reconnect")
                        num_rec +=1
                    if num_rec == MAXIMUM_RECONNECTIONS:
                        return None, None, None, None
                if chunk_name == -1:
                    break

                with open(chunk_name, 'rb') as f:
                    df = pickle.load(f)

                current_shape = df.shape[0]

                if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:

                    # Have to resize the reserved space
                    if ( current_lower_bound + current_shape ) > approximate_rows:
                        extend_value = df_columns[0]
                        for nm in nms_without_key:
                            df_columns[nm] = append(df_columns[nm], [ extend_value for i in range(current_shape*3)])
                        extend_value = key_pointer_str['key'][0]
                        key_pointer_str['key'] = append(key_pointer_str['key'], [ extend_value for i in range(current_shape*3)])
                        extend_value = key_pointer_str['pointer'][0]
                        key_pointer_str['pointer'] = append(key_pointer_str['pointer'], [extend_value for i in range(current_shape * 3)])

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

                # if am_i_straggler:
                #     print("straggler will sleep {} s".format(sleep_time))
                #     time.sleep(sleep_time)

            medium_time = time.time()

            # Global sort
            print("Sorting")
            sorted_indexes = argsort(
                key_pointer_str['key'][0:current_lower_bound],
                kind='mergesort')
            sorted_indexes = sorted_indexes.astype('uint32')

            key_pointer_str['pointer'] = \
                key_pointer_str['pointer'][0:current_lower_bound][
                    sorted_indexes]
            key_pointer_str['key'] = \
                key_pointer_str['key'][0:current_lower_bound][sorted_indexes]
            print("Sorted keys")

            del sorted_indexes

            if parser_data['low_memory']:
                with open("keys", "wb") as f:
                    pickle.dump(key_pointer_str['key'], f)
                del key_pointer_str['key']

            for nm in nms_without_key:
                df_columns[nm] = df_columns[nm][key_pointer_str['pointer']]

            del key_pointer_str['pointer']
            if parser_data['low_memory']:
                with open("keys", "rb") as f:
                    key_pointer_str = {'key': pickle.load(f)}

            df = _create_dataframe(df_columns, key_pointer_str['key'], types, key_nm)

            return df, read_cnks, medium_time-my_start_time, time.time()-medium_time

        with ThreadPoolExecutor(max_workers=(DEFAULT_THREAD_NUM)) as pool:
            pool.submit(_chunk_producer, i)
            consumer_thread_future = pool.submit(_chunk_consumer)
            ed, read_cnks, read_time, sort_time = consumer_thread_future.result()

        if ed is None:
            print("Received end signal")
            return -1, times
        times.append(read_time)
        times.append(sort_time)
        after_readt = time.time()
        total_size = ed.shape[0]

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
        else:

            print("Segment shape after {}".format(ed.shape))

            # Pd
            num_rows = total_size
            mem = ed.memory_usage(index=False)
            final_segment_size_bytes = sum([mem[i] for i in range(len(ed.columns))])
            avg_bytes_per_row = final_segment_size_bytes / num_rows

            print("Row size {}; total segment size {}; minimum upload size {}".format(avg_bytes_per_row,
                                                                                      final_segment_size_bytes,
                                                                                      MIN_MULTIPART_SIZE * GRANULE_SIZE_TO_BYTES))

            if parser_data['low_memory']:
                _multipart_upload(ibm_cos, my_output_path, output_bucket, ed, DEFAULT_MULTIPART_NUMBER, segm_i)
            else:
                ibm_cos.put_object(Bucket=output_bucket,
                                   Key="{}/{}.pickle".format(my_output_path, segm_i),
                                   Body=pickle.dumps(ed))

        times.append(time.time() - after_readt)

    return total_size, times
