from queue import Queue as threadPoolQueue
from queue import Empty
import time
from concurrent.futures.thread import ThreadPoolExecutor
import pickle
import numpy as np
from numpy import diff, empty, array, argsort, concatenate
from pandas import DataFrame
from ibm_botocore.exceptions import ClientError
from time import sleep

from pywren_ibm_cloud.compute.backends.ibm_cf.config import MAX_CONCURRENT_WORKERS
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.patch import patch_map_monitor, unpatch_map_monitor
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _check_if_speculative, _check_end_signal, \
    _upload_progress
from pywren_ibm_cloud.sort.config import GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION, MIN_MULTIPART_SIZE, \
    QUEUE_TIMEOUT
from pywren_ibm_cloud.sort.tests.basic_sort import _reduce_partitions_basic

from pywren_ibm_cloud.sort import utils as ut

MAX_RETRIES = 10
DEFAULT_THREAD_NUM = 2

is_sorted_np = lambda x: (diff(x) >= 0).all()


def reduce_partition_segments(pw, num_segments, parser_info_path, parser_info_bucket, speculative=False):

    segm_groups = list()
    for i in range(min(num_segments, MAX_CONCURRENT_WORKERS)):
        segm_groups.append({'segms': list()})
    for i in range(num_segments):
        segm_groups[i % min(num_segments, PAR_LEVEL_LIMIT_PER_FUNCTION)]['segms'].append(i)

    if speculative:
        patch_map_monitor(pw)
        _reduce_partitions = _reduce_partitions_monitored
    else:
        _reduce_partitions = _reduce_partitions_standalone

    # TODO: remove
    # _reduce_partitions = _reduce_partitions_basic

    arguments = _create_reduced_args(segm_groups, parser_info_bucket, parser_info_path)

    futures = pw.map(_reduce_partitions, arguments)

    res = pw.get_result(futures)

    if speculative:
        unpatch_map_monitor(pw)

    print([ v[0] for v in res])
    print(sum([ v[0] for v in res]))

    kind = "tim"
    numr = np.array([v[0] for v in res])
    fmean, std = numr.mean(), numr.std()
    print(f'    {kind}: {fmean}±{std} float64 per worker, {len(res)} workers')
    times = np.array([v[1] for v in res]) * 1e6
    fmean, std = times.mean(), times.std()
    print(f'    {kind}: Full {fmean:.3f}±{std:.3f} us per worker')


def _create_reduced_args(segm_groups, parser_info_bucket, parser_info_path):
    arguments = [
        { 'args' : {
            'index' : idx,
            'segms' : segms['segms'],
            'parser_info_path': parser_info_path,
            'parser_info_bucket': parser_info_bucket
        }}
        for idx, segms in enumerate(segm_groups)
    ]
    return arguments


def _reduce_partitions_standalone(args, ibm_cos):

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

            return { 'path' : path, 'gr_num': parts['nums'][int(segm_i)], 'row_num': parts['row_num'][int(segm_i)] }

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

        def _chunk_producer(t_i):
            correctly_reads = 0
            while not q_reads.empty():
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
            retry_count=0

            while read_cnks < len(parts_info):
                try:

                    chunk_name = q_writes.get(block=True, timeout=QUEUE_TIMEOUT)

                    with open(chunk_name, 'rb') as f:
                        df = pickle.load(f)

                    current_shape = df.shape[0]

                    print("Read {} rows from {}".format(current_shape, chunk_name))

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

                    print("porcessed chunk {} at {} s".format(read_cnks, time.time() - start_time))

                except Empty:

                    if retry_count == MAX_RETRIES:
                        print("WARNING: chunk producer ended before expected")
                        break

                    if q_reads.empty():
                        retry_count +=1
                        sleep(0.01)

            sorted_indexes = argsort(
                key_pointer_str['key'][
                0:(current_lower_bound)],
                kind='mergesort')

            key_pointer_str['pointer'][0:(current_lower_bound)] = \
                key_pointer_str['pointer'][0:(current_lower_bound)][sorted_indexes]

            key_pointer_str['key'][0:(current_lower_bound)] = \
                key_pointer_str['key'][0:(current_lower_bound)][sorted_indexes]

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
        time1 = time.time()
        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer, info)
            for i in range(DEFAULT_THREAD_NUM - 1):
                print("submitted chunk producer {}".format(i))
                fts.append(pool.submit(_chunk_producer, i))
            for ft in fts:
                print(ft)
            obj = consumer_thread_future.result()

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
        total_size = obj.shape[0]

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
            ret_dict['correctness'] = ret_dict['correctness'] & True

        else:

            # Pd
            obj = obj.sort_values(str(parser_data['column_numbers'][0]))

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
                                       Key="{}/{}.pickle".format(output_path, segm_i),
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
                                                   Key="{}/{}.pickle".format(output_path, segm_i),
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
                                                            Key="{}/{}.pickle".format(output_path,
                                                                                        segm_i))['UploadId']
                print("{} parts".format(len(obj_parts)))

                def _upload_segment_part(part_i):
                    etag = -1
                    print("Uploading {}".format(part_i))
                    try:
                        etag = ibm_cos.upload_part(Bucket=output_bucket,
                                                   Key="{}/{}.pickle".format(output_path, segm_i),
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
                                                               Key="{}/{}.pickle".format(output_path, segm_i),
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
                                                   Key="{}/{}.pickle".format(output_path, segm_i),
                                                   UploadId=upload_id)
                    after_writet = before_writet
                else:
                    multipart_json = {'Parts': []}
                    for t in range(len(etags)):
                        multipart_json['Parts'].append({'ETag': etags[t], 'PartNumber': t + 1})
                    ibm_cos.complete_multipart_upload(Bucket=output_bucket,
                                                      Key="{}/{}.pickle".format(output_path, segm_i),
                                                      UploadId=upload_id,
                                                      MultipartUpload=multipart_json)

                    after_writet = time.time()
                    print("DEV:::REDUCE SEGMENT; {} writes - {} MB written - {} s time".format(len(obj_parts),
                                                                                               final_segment_size_bytes / (
                                                                                                       1024 ** 2),
                                                                                               after_writet - before_writet))
                    print("Multipart upload of segment {} completed".format(segm_i))

    return [ total_size, after_readt-time1 ]


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