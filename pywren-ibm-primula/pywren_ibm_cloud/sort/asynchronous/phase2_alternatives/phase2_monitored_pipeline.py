from queue import Queue as threadPoolQueue
import time
from concurrent.futures.thread import ThreadPoolExecutor
import pickle
from numpy import empty, array, argsort, concatenate, append
from pandas import DataFrame
from ibm_botocore.exceptions import ClientError
from time import sleep

from pywren_ibm_cloud.sort.asynchronous.config import MAX_TRIALS_PER_PARTITION, \
    SLEEP_TIME_ASYNC
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _check_end_signal, \
    _upload_progress, _check_if_speculative
from pywren_ibm_cloud.sort.config import GRANULE_SIZE_TO_BYTES, MIN_MULTIPART_SIZE, \
    DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN, PHASE_2_MULTIPLIER, OUTPUT_GRANULARITY, MAX_RETRIES


def _reduce_partitions(args, ibm_cos):
    segment_i = args['arguments']['args']['index']
    segms = args['arguments']['args']['segms']

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['arguments']['args']['parser_info_bucket'],
                                                  Key=args['arguments']['args']['parser_info_path'])['Body'].read())

    input_bucket = parser_data['input_bucket']
    output_bucket = parser_data['output_bucket']
    input_path = parser_data['input_path']
    intermediate_path = parser_data['intermediate_path']
    output_path = parser_data['output_path']
    granule_size = OUTPUT_GRANULARITY
    partition_number = parser_data['num_workers_phase1']

    fname, is_speculative = _check_if_speculative(ibm_cos, args, int(segment_i))
    if is_speculative:
        my_output_path = "{}/{}s".format(output_path, segment_i)
    else:
        my_output_path = "{}/{}".format(output_path, segment_i)

    # sleep_time = 10
    # try:
    #     ibm_cos.get_object(Bucket=input_bucket,
    #                        Key="straggler_p2_{}".format(segment_i))
    #     am_i_straggler = True
    #     ibm_cos.delete_object(Bucket=input_bucket,
    #                           Key="straggler_p2_{}".format(segment_i))
    # except ClientError as ex:
    #     am_i_straggler = False

    # print("straggler: {} - speculative: {}".format(am_i_straggler, is_speculative))

    for segm_i in segms:

        print("Reducing segment {} from {} partition".format(segm_i, partition_number))

        # thread pool
        q_writes = threadPoolQueue()
        q_finish = threadPoolQueue()
        sizeq = threadPoolQueue()

        types = dict()
        for i in range(len(parser_data['dtypes'])):
            types[str(i)] = parser_data['dtypes'][i]
        nms = [str(i) for i in range(len(parser_data['dtypes']))]

        before_readt = time.time()

        # Generate output path dictionary
        def _chunk_producer(t_i):

            correctly_reads = 0
            parts_counter = 0
            trials = [0 for i in range(partition_number)]
            pendent_partition_i = list(range(partition_number))
            partition_pos = -1
            while len(pendent_partition_i) is not 0:

                # Get next partition
                partition_pos = (partition_pos + 1) % len(pendent_partition_i)
                p_i = pendent_partition_i[partition_pos]

                if q_finish.empty() is not True:
                    return None

                try:
                    output_path = "{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, p_i)
                    cos_read_path = ibm_cos.get_object(Bucket=output_bucket, Key=output_path)['Body'].read()
                    path = pickle.loads(cos_read_path)
                    # print("Read output path {}".format(path))

                    part_num_name = "{}/part_number.pickle".format(path)
                    cos_read_parts = ibm_cos.get_object(Bucket=output_bucket, Key=part_num_name)['Body'].read()
                    parts = pickle.loads(cos_read_parts)

                    for c_i in range(parts['nums'][int(segm_i)]):

                        if q_finish.empty() is not True:
                            return None

                        read_part = ibm_cos.get_object(Bucket=output_bucket,
                                                       Key="{}/sgm{}/gran{}.pickle".format(path, segm_i, c_i))

                        part_size = read_part['ContentLength']
                        print("Read size {}".format(read_part['ContentLength']))

                        sizeq.put(part_size)
                        with open("p{}_c{}".format(p_i, c_i), 'wb') as f:
                            f.write(read_part['Body'].read())

                        q_writes.put_nowait("p{}_c{}".format(p_i, c_i))
                        print("q_writes content {}".format(q_writes.queue))

                        correctly_reads += 1

                    pendent_partition_i.remove(p_i)

                except ClientError as ex:
                    if ex.response['Error']['Code'] == 'NoSuchKey':
                        trials[p_i] += 1
                        if trials[p_i] > MAX_TRIALS_PER_PARTITION:
                            print("Could not read partition {}".format(p_i))
                            q_finish.put(1)
                            raise BufferError
                        # Mapper has still not finished
                        # sleep_time = len(pendent_partition_i) / TIME_SLEEP_DIV
                        time.sleep(SLEEP_TIME_ASYNC)
                    else:
                        raise

            return correctly_reads

        def _chunk_consumer():

            my_allocation_margin = DATAFRAME_ALLOCATION_MARGIN * PHASE_2_MULTIPLIER
            # my_allocation_margin = DATAFRAME_ALLOCATION_MARGI

            read_cnks = 0

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

            while not q_writes.empty() :

                chunk_name = q_writes.get(block=True, timeout=100)

                fs = _check_end_signal(ibm_cos, args, fname)
                if fs or not q_finish.empty():
                    q_finish.put(1)
                    return None, None

                print("got name {}".format(chunk_name))

                with open(chunk_name, 'rb') as f:
                    df = pickle.load(f)

                current_shape = df.shape[0]
                print("adding {} rows to buffer size {}, occupated {}".format(current_shape, approximate_rows, current_lower_bound))

                if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                    # Have to resize the reserved space
                    if ( current_lower_bound + current_shape ) > approximate_rows:
                        print("resizing {} to {}".format(approximate_rows, approximate_rows + (current_shape * 3)))
                        extend_value = df_columns[0]
                        for nm in nms_without_key:
                            df_columns[nm] = append(df_columns[nm], [ extend_value for i in range(current_shape*3)])
                        extend_value = key_pointer_str['key'][0]
                        key_pointer_str['key'] = append(key_pointer_str['key'], [ extend_value for i in range(current_shape*3)])
                        extend_value = key_pointer_str['pointer'][0]
                        key_pointer_str['pointer'] = append(key_pointer_str['pointer'], [extend_value for i in range(current_shape * 3)])
                        print("Extending buffer")
                        print("new array size {}".format(key_pointer_str['key'].shape[0]))

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

                key_pointer_str['pointer'][0:(current_lower_bound + current_shape)] = \
                    key_pointer_str['pointer'][0:(current_lower_bound + current_shape)][sorted_indexes]

                key_pointer_str['key'][0:(current_lower_bound + current_shape)] = \
                    key_pointer_str['key'][0:(current_lower_bound + current_shape)][sorted_indexes]

                current_lower_bound = current_lower_bound + current_shape

                read_cnks += 1
                print("processed chunk {} at {} s".format(read_cnks, time.time() - start_time))

                if not is_speculative:
                    _upload_progress(ibm_cos, segment_i, (current_lower_bound) / approximate_rows, time.time() - start_time,
                                     args)

                # if am_i_straggler:
                #     print("straggler will sleep {} s".format(sleep_time))
                #     time.sleep(sleep_time)

                fs = _check_end_signal(ibm_cos, args, fname)
                if fs:
                    q_finish.put(1)
                    return None, None

            if parser_data['column_numbers'] is 0:
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
            return df, read_cnks

        with ThreadPoolExecutor(max_workers=(DEFAULT_THREAD_NUM)) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer)
            print("submitted chunk producer {}".format(i))
            producer_thread_future = pool.submit(_chunk_producer, i)
            a = producer_thread_future.future()
            obj, read_cnks = consumer_thread_future.result()

        if obj is None:
            print("Received end signal")
            return -1

        after_readt = time.time()
        print("DEV:::REDUCE SEGMENT; {} reads - {} MB read - {} s time".format(read_cnks,
                                                                               sum(list(
                                                                                   sizeq.queue)) / (
                                                                                       1024 ** 2),
                                                                               after_readt - before_readt
                                                                               ))
        upload_done = False
        total_size = obj.shape[0]

        finish_signal = _check_end_signal(ibm_cos, args, fname)
        if finish_signal:
            return None

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
            return 0
        else:

            # Pd
            obj = obj.sort_values(str(parser_data['column_numbers'][0]))

            total_rows = obj.shape[0]

            print("Segment shape after {}".format(obj.shape))

            # Pd
            num_rows = total_size
            mem = obj.memory_usage(index=False)
            final_segment_size_bytes = sum([mem[i] for i in range(len(obj.columns))])
            avg_bytes_per_row = final_segment_size_bytes / num_rows

            print("Row size {}; total segment size {}; minimum upload size {}".format(avg_bytes_per_row,
                                                                                      final_segment_size_bytes,
                                                                                      MIN_MULTIPART_SIZE * GRANULE_SIZE_TO_BYTES))
            if (granule_size == -1) or (final_segment_size_bytes < MIN_MULTIPART_SIZE * GRANULE_SIZE_TO_BYTES):
                try:
                    before_writet = time.time()
                    ibm_cos.put_object(Bucket=output_bucket,
                                       Key="{}/{}.pickle".format(my_output_path, segm_i),
                                       Body=pickle.dumps(obj))
                    upload_done = True
                except ClientError:
                    # Too many requests error
                    available_retries = MAX_RETRIES
                    while not upload_done:
                        if available_retries <= 0:
                            print("Could not upload segment {}".format(segm_i))
                            break
                        else:
                            sleep(0.05)
                            try:
                                ibm_cos.put_object(Bucket=output_bucket,
                                                   Key="{}/{}.pickle".format(my_output_path, segm_i),
                                                   Body=pickle.dumps(obj))
                                upload_done = True
                            except ClientError:
                                available_retries -= 1
                after_writet = time.time()
            else:
                print("Multipart upload of segment {}".format(segm_i))

                # Generate segment byte object
                granule_size = granule_size * GRANULE_SIZE_TO_BYTES

                obj = pickle.dumps(obj)
                obj_parts = [obj[i:i + granule_size] for i in range(0, len(obj), granule_size)]
                upload_id = ibm_cos.create_multipart_upload(Bucket=output_bucket,
                                                            Key="{}/{}.pickle".format(my_output_path,
                                                                                      segm_i))['UploadId']
                print("{} parts".format(len(obj_parts)))

                def _upload_segment_part(part_i):
                    etag = -1
                    print("Uploading {}".format(part_i))
                    try:
                        etag = ibm_cos.upload_part(Bucket=output_bucket,
                                                   Key="{}/{}.pickle".format(my_output_path, segm_i),
                                                   Body=obj_parts[part_i],
                                                   UploadId=upload_id,
                                                   PartNumber=(part_i + 1))['ETag']
                        print("Uploaded part {}".format(part_i))
                    except ClientError:
                        # Too many requests error
                        available_retries = MAX_RETRIES
                        while not upload_done:
                            if available_retries <= 0:
                                break
                            else:
                                sleep(0.05)
                                try:
                                    print("Retrying part {}".format(part_i))
                                    etag = ibm_cos.upload_part(Bucket=output_bucket,
                                                               Key="{}/{}.pickle".format(my_output_path, segm_i),
                                                               Body=pickle.dumps(obj_parts[part_i]),
                                                               UploadId=upload_id,
                                                               PartNumber=(part_i + 1))['ETag']
                                except ClientError:
                                    available_retries -= 1
                    return etag

                before_writet = time.time()
                etags = list(map(_upload_segment_part, range(len(obj_parts))))

                print("Etags")
                print(etags)

                if -1 in etags:
                    print("Could not upload segment {}".format(segm_i))
                    ibm_cos.abort_multipart_upload(Bucket=output_bucket,
                                                   Key="{}/{}.pickle".format(my_output_path, segm_i),
                                                   UploadId=upload_id)
                    after_writet = before_writet
                else:
                    multipart_json = {'Parts': []}
                    for t in range(len(etags)):
                        multipart_json['Parts'].append({'ETag': etags[t], 'PartNumber': t + 1})
                    ibm_cos.complete_multipart_upload(Bucket=output_bucket,
                                                      Key="{}/{}.pickle".format(my_output_path, segm_i),
                                                      UploadId=upload_id,
                                                      MultipartUpload=multipart_json)

                    after_writet = time.time()
                    print("DEV:::REDUCE SEGMENT; {} writes - {} MB written - {} s time".format(len(obj_parts),
                                                                                               final_segment_size_bytes / (
                                                                                                       1024 ** 2),
                                                                                               after_writet - before_writet))
                    print("Multipart upload of segment {} completed".format(segm_i))

    return total_size
