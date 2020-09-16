from queue import Queue
from queue import Empty
import time
from concurrent.futures.thread import ThreadPoolExecutor
import pickle
from numpy import empty, array, argsort, concatenate, append
from ibm_botocore.exceptions import ClientError

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
    segment_i = args['arguments']['args']['index']
    segms = args['arguments']['args']['segms']

    start_time = time.time()
    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['arguments']['args']['parser_info_bucket'],
                                                  Key=args['arguments']['args']['parser_info_path'])['Body'].read())

    output_bucket = parser_data['output_bucket']
    intermediate_path = parser_data['intermediate_path']
    output_path = parser_data['output_path']
    partition_number = parser_data['num_workers_phase1']


    #######################################################
    num_buckets = len(parser_data['bucket_names'])
    bucket_list = parser_data['bucket_names']
    workers_p_bucket = partition_number / num_buckets

    #######################################################

    fname, is_speculative = _check_if_speculative(ibm_cos, args, int(segment_i))
    if is_speculative:
        my_output_path = "{}/{}s".format(output_path, segment_i)
    else:
        my_output_path = "{}/{}".format(output_path, segment_i)


    for segm_i in segms:

        print("Reducing segment {} from {} partition".format(segm_i, partition_number))

        ####################
        my_bucket = bucket_list[int(segm_i / workers_p_bucket)]
        print("my_bucket: {}".format(my_bucket))
        ####################

        # thread pool
        q_writes = Queue()
        q_finish = Queue()

        types = dict()
        for i in range(len(parser_data['dtypes'])):
            types[str(i)] = parser_data['dtypes'][i]
        nms = [str(i) for i in range(len(parser_data['dtypes']))]

        # Generate output path dictionary
        def _chunk_producer(t_i):

            print("started chunk producer")

            before_readt = time.time()
            pendent_partition_i = list(range(partition_number))
            partition_pos = segm_i % partition_number
            while len(pendent_partition_i) is not 0:

                p_i = pendent_partition_i[partition_pos]

                try:
                    output_path = "{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, p_i)
                    cos_read_path = ibm_cos.get_object(Bucket=output_bucket, Key=output_path)['Body'].read()
                    next_path = pickle.loads(cos_read_path)
                    part_num_name = "{}/part_number.pickle".format(next_path)
                    cos_read_parts = ibm_cos.get_object(Bucket=output_bucket, Key=part_num_name)['Body'].read()
                    parts = pickle.loads(cos_read_parts)

                    for c_i in range(parts['nums'][int(segm_i)]):

                        if q_finish.empty() is not True:
                            q_writes.put(-2)
                            return None

                        read_part = ibm_cos.get_object(Bucket=my_bucket,
                                                       Key="{}/sgm{}/gran{}.pickle".format(next_path, segm_i, c_i))

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
                if len(pendent_partition_i) is not 0:
                    partition_pos = (partition_pos + 1) % len(pendent_partition_i)

            q_writes.put(-1)
            return None

        def _chunk_consumer():

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
                            return -2, None
                        break
                    except TimeoutError:
                        print("reconnect")
                        num_rec +=1
                    except Empty:
                        print("reconnect")
                        num_rec +=1
                    if num_rec == MAXIMUM_RECONNECTIONS:
                        return -1, None
                if chunk_name == -1:
                    break

                if _check_end_signal(ibm_cos, args, fname):
                    q_finish.put(1)
                    return None, None

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

                if not is_speculative:
                    _upload_progress(ibm_cos, segment_i, (current_lower_bound) / approximate_rows, time.time() - start_time,
                                     args)
                # if am_i_straggler:
                #     print("straggler will sleep {} s".format(sleep_time))
                #     time.sleep(sleep_time)

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

            return df, read_cnks

        with ThreadPoolExecutor(max_workers=(DEFAULT_THREAD_NUM)) as pool:
            pool.submit(_chunk_producer, i)
            consumer_thread_future = pool.submit(_chunk_consumer)
            ed, read_cnks = consumer_thread_future.result()

        if read_cnks is None:
            print("Ended reducer")
            return ed

        after_readt = time.time()
        print("DEV:::REDUCE SEGMENT; {} reads - {} s time".format(read_cnks, after_readt - start_time))
        total_size = ed.shape[0]

        if _check_end_signal(ibm_cos, args, fname):
            return None

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
        else:
            print("Segment shape after {}".format(ed.shape))
            # Pd
            if parser_data['low_memory']:
                _multipart_upload(ibm_cos, my_output_path, my_bucket, ed, DEFAULT_MULTIPART_NUMBER, segm_i)
            else:
                ibm_cos.put_object(Bucket=my_bucket,
                                   Key="{}/{}.pickle".format(my_output_path, segm_i),
                                   Body=pickle.dumps(ed))

    return total_size
