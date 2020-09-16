import os
from queue import Queue as threadPoolQueue
import time
from concurrent.futures.thread import ThreadPoolExecutor
import pickle
from numpy import empty, array, argsort, concatenate
from pandas import DataFrame
from ibm_botocore.exceptions import ClientError
from time import sleep

from pywren_ibm_cloud.sort.asynchronous.config import TIME_SLEEP_DIV, MAX_TRIALS_PER_PARTITION
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.config import GRANULE_SIZE_TO_BYTES, MIN_MULTIPART_SIZE, \
    DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN, MAX_RETRIES


def _reduce_partitions(args, ibm_cos):

    print("new reducer")
    print(args)

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

    for segm_i in segms:

        print("Reducing segment {} from {} partition".format(segm_i, partition_number))

        # thread pool
        q_writes = threadPoolQueue()
        ended_producers = threadPoolQueue(DEFAULT_THREAD_NUM-1)
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

                try:
                    output_path = "{}/{}/{}.pickle".format(intermediate_path, SHUFFLE_OUTPUT_PREFIX, p_i)
                    cos_read_path = ibm_cos.get_object(Bucket=output_bucket, Key=output_path)['Body'].read()
                    path = pickle.loads(cos_read_path)
                    # print("Read output path {}".format(path))

                    part_num_name = "{}/part_number.pickle".format(path)
                    cos_read_parts = ibm_cos.get_object(Bucket=output_bucket, Key=part_num_name)['Body'].read()
                    parts = pickle.loads(cos_read_parts)

                    for c_i in range(parts['nums'][int(segm_i)]):

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
                            return 0
                        # Mapper has still not finished
                        sleep_time = len(pendent_partition_i) / TIME_SLEEP_DIV
                        time.sleep(sleep_time)
                    else:
                        raise

            ended_producers.put(t_i)
            return correctly_reads

        def _chunk_consumer():

            my_allocation_margin = DATAFRAME_ALLOCATION_MARGIN * 2

            read_cnks = 0

            start_time = time.time()


            nms_without_key = nms.copy()
            nms_without_key.remove(nms[parser_data['column_numbers'][0]])

            if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                df_columns = {nm: empty(
                    int(parser_data['total_rows_approx'] / parser_data['num_workers_phase2']) + int(
                        parser_data['total_rows_approx'] / parser_data['num_workers_phase2'] * my_allocation_margin), dtype=types[nm])
                    for nm in nms_without_key}
                key_pointer_str = {
                    'key': empty(int(parser_data['total_rows_approx'] / parser_data['num_workers_phase2']) + int(
                        parser_data['total_rows_approx'] / parser_data['num_workers_phase2'] * my_allocation_margin),
                                 dtype=types[nms[parser_data['column_numbers'][0]]]),
                    'pointer': empty(int(parser_data['total_rows_approx'] / parser_data['num_workers_phase2']) + int(
                        parser_data['total_rows_approx'] / parser_data['num_workers_phase2'] * my_allocation_margin), dtype='int32')
                }
            else:
                df_columns = {nm: empty(0, dtype=types[nm])
                              for nm in nms}
                key_pointer_str = {
                    'key': empty(0, dtype=types[nms[parser_data['column_numbers'][0]]]),
                    'pointer': empty(0, dtype='int32')
                }

            current_lower_bound = 0

            while not q_writes.empty() or ended_producers.qsize() < (DEFAULT_THREAD_NUM-1):

                chunk_name = q_writes.get(block=True)
                print("got name {}".format(chunk_name))

                # Read intermediate file.
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

                key_pointer_str['pointer'][0:(current_lower_bound + current_shape)] = \
                    key_pointer_str['pointer'][0:(current_lower_bound + current_shape)][sorted_indexes]

                key_pointer_str['key'][0:(current_lower_bound + current_shape)] = \
                    key_pointer_str['key'][0:(current_lower_bound + current_shape)][sorted_indexes]

                current_lower_bound = current_lower_bound + current_shape

                read_cnks += 1
                print("processed chunk {} at {} s".format(read_cnks, time.time() - start_time))

            for nm in nms_without_key:
                df_columns[nm] = \
                    df_columns[nm][0:current_lower_bound][
                        key_pointer_str['pointer'][0:current_lower_bound]]

            df_columns[nms[parser_data['column_numbers'][0]]] = \
                key_pointer_str['key'][0:current_lower_bound]

            df = DataFrame(df_columns)

            # return df, sort_times
            return df, read_cnks


        with ThreadPoolExecutor(max_workers=(DEFAULT_THREAD_NUM)) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer)
            for i in range(DEFAULT_THREAD_NUM - 1):
                 print("submitted chunk producer {}".format(i))
                 pool.submit(_chunk_producer, i)
            obj, read_cnks = consumer_thread_future.result()


        after_readt = time.time()
        print("DEV:::REDUCE SEGMENT; {} reads - {} MB read - {} s time".format(read_cnks,
                                                                               sum(list(
                                                                                   sizeq.queue)) / (
                                                                                           1024 ** 2),
                                                                               after_readt - before_readt
                                                                               ))
        upload_done = False
        total_size = obj.shape[0]


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
                                                                                      MIN_MULTIPART_SIZE * GRANULE_SIZE_TO_BYTES ))
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