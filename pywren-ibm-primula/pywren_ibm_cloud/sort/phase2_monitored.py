import pickle
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue as threadPoolQueue, Empty
from time import sleep

from ibm_botocore.exceptions import ClientError
from numpy import concatenate, argsort
from numpy.core._multiarray_umath import empty, array
from pandas import DataFrame
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _check_if_speculative, _check_end_signal, \
    _upload_progress
from pywren_ibm_cloud.sort.config import PAR_LEVEL_LIMIT_PER_FUNCTION, QUEUE_TIMEOUT, MIN_MULTIPART_SIZE, \
    GRANULE_SIZE_TO_BYTES, MAX_RETRIES
from pywren_ibm_cloud.sort.config import DEFAULT_THREAD_NUM


def _reduce_partitions_monitored(args, ibm_cos):

    segment_i = args['arguments']['args']['index']
    segms = args['arguments']['args']['segms']

    parser_data = pickle.loads(ibm_cos.get_object(Bucket=args['arguments']['args']['parser_info_bucket'],
                                                  Key=args['arguments']['args']['parser_info_path'])['Body'].read())

    print("Assigned segments {}".format(segms))
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

    # Protocol to know if i am speculative.
    fname, is_speculative = _check_if_speculative(ibm_cos, args, int(segment_i))
    if is_speculative:
        my_output_path = "{}/{}s".format(output_path, segment_i)
    else:
        my_output_path = "{}/{}".format(output_path, segment_i)

    read_time = 0
    write_time = 0
    ret_dict = {'size': 0, 'correctness': True, 'upload': False}
    for segm_i in segms:

        print("Reducing segment {}".format(segm_i))

        # Generate output path dictionary
        def _extract_segment_info(partition_i):
            path = pickle.loads(ibm_cos.get_object(Bucket=output_bucket,
                                                   Key="{}/{}/{}.pickle".format(intermediate_path,
                                                                                SHUFFLE_OUTPUT_PREFIX,
                                                                                partition_i))['Body'].read())
            print("Read output path {}".format(path))
            parts = pickle.loads(ibm_cos.get_object(Bucket=output_bucket,
                                                    Key="{}/part_number.pickle".format(path))['Body'].read())

            return {'path': path, 'gr_num': parts['nums'][int(segm_i)], 'row_num': parts['row_num'][int(segm_i)] }

        # Extract parts for each segment
        with ThreadPoolExecutor(min(int(partition_number), PAR_LEVEL_LIMIT_PER_FUNCTION)) as pool:
            res = list(pool.map(_extract_segment_info, range(int(partition_number))))

        paths = [x['path'] for x in res]
        granule_numbers = [x['gr_num'] for x in res]
        row_num = sum([x['row_num'] for x in res])

        parts_info = []
        total_counter = 0
        for c in range(int(partition_number)):
            for g in range(granule_numbers[c]):
                parts_info.append([c, g])

        # thread pool
        q_writes = threadPoolQueue(len(parts_info))
        q_reads = threadPoolQueue(len(parts_info))
        sizeq = threadPoolQueue(len(parts_info))

        for inf in parts_info:
            q_reads.put_nowait(inf)
            print(inf)

        print("q_reads content {}".format(list(q_reads.queue)))

        types = dict()
        for i in range(len(parser_data['dtypes'])):
            types[str(i)] = parser_data['dtypes'][i]
        nms = [str(i) for i in range(len(parser_data['dtypes']))]

        before_readt = time.time()
        finish_signal = False

        def _chunk_producer(t_i):
            correctly_reads = 0
            while not q_reads.empty():

                if finish_signal is True:
                    return None

                c_g = q_reads.get()
                path = paths[int(c_g[0])]
                print("Reading parallel chunk {}".format(c_g))
                print("(inside producer) q_reads content {}".format(list(q_reads.queue)))
                partition_num = c_g[0]
                chunk_num = c_g[1]

                read_part = ibm_cos.get_object(Bucket=output_bucket,
                                               Key="{}/sgm{}/gran{}.pickle".format(path,
                                                                                   segm_i,
                                                                                   chunk_num))
                part_size = read_part['ContentLength']
                print("Read size {}".format(read_part['ContentLength']))

                sizeq.put(part_size)
                with open("p{}_c{}".format(partition_num, chunk_num), 'wb') as f:
                    f.write(read_part['Body'].read())

                q_writes.put("p{}_c{}".format(partition_num, chunk_num))
                correctly_reads += 1

            return correctly_reads

        def _chunk_consumer(info):
            read_cnks = 0

            start_time = time.time()
            key_nm = nms[parser_data['column_numbers'][0]]

            # For calculation of each reduce time.
            # reduce_times = []
            if types[nms[parser_data['column_numbers'][0]]] not in ['bytes', 'str', 'object']:
                df_columns = {
                    nm: empty(info['row_num'], dtype=types[nm]) for nm in nms
                }
                key_pointer_str = {
                    'key': empty(info['row_num'], dtype=types[nms[parser_data['column_numbers'][0]]]),
                    'pointer': empty(info['row_num'], dtype='int32')
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
            accumulated_size = 0
            retry_count=0
            while read_cnks < len(parts_info):
                try:
                    chunk_name = q_writes.get(block=True, timeout=QUEUE_TIMEOUT)

                    finish_signal = _check_end_signal(ibm_cos, args, fname)
                    if finish_signal:
                        return None

                    with open(chunk_name, 'rb') as f:
                        df = pickle.load(f)

                    chunk_sort_start_time = time.time()

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
                    print("porcessed chunk {} at {} s".format(read_cnks, time.time() - start_time))

                    if not is_speculative:
                        _upload_progress(ibm_cos, segment_i, current_shape / info['row_num'], time.time() - start_time,
                                         args)
                    finish_signal = _check_end_signal(ibm_cos, args, fname)
                    if finish_signal:
                        return None

                except Empty:

                    if retry_count == MAX_RETRIES:
                        print("WARNING: chunk producer ended before expected")
                        break

                    if q_reads.empty():
                        retry_count +=1
                        sleep(0.01)

            for nm in nms_without_key:
                df_columns[nm] = \
                    df_columns[nm][0:current_lower_bound][
                        key_pointer_str['pointer'][0:current_lower_bound]]

            df_columns[nms[parser_data['column_numbers'][0]]] = \
                key_pointer_str['key'][0:current_lower_bound]

            df = DataFrame(df_columns)

            # return df, sort_times
            return df

        info = {'row_num': row_num, 'total_num': len(parts_info)}
        fts = []
        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer, info)
            for i in range(DEFAULT_THREAD_NUM - 1):
                print("submitted chunk producer {}".format(i))
                fts.append(pool.submit(_chunk_producer, i))
            for ft in fts:
                print(ft)
            obj = consumer_thread_future.result()

        if obj is None:
            print("Received end signal")
            return -1

        # return -1
        after_readt = time.time()
        print("DEV:::REDUCE SEGMENT; {} reads - {} MB read - {} s time".format(len(parts_info),
                                                                                           sum(list(
                                                                                               sizeq.queue)) / (
                                                                                                       1024 ** 2),
                                                                                           after_readt - before_readt
                                                                                           ))
        after_readt = time.time()
        read_time += after_readt - before_readt

        upload_done = False
        sort_is_correct = False
        total_size = obj.shape[0]

        finish_signal = _check_end_signal(ibm_cos, args, fname)
        if finish_signal:
            return None

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
            ret_dict['correctness'] = ret_dict['correctness'] & True

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