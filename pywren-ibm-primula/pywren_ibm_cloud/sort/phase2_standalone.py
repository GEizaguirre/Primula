import pickle
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue as threadPoolQueue, Empty
from time import sleep

from numpy import concatenate, argsort
from numpy.core._multiarray_umath import empty, array
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.config import PAR_LEVEL_LIMIT_PER_FUNCTION, QUEUE_TIMEOUT, MIN_MULTIPART_SIZE, \
    GRANULE_SIZE_TO_BYTES, MAX_RETRIES, DEFAULT_MULTIPART_NUMBER,DEFAULT_THREAD_NUM
from pywren_ibm_cloud.sort.utils import _create_dataframe, _multipart_upload


def _reduce_partitions_standalone(args, ibm_cos):

    start_time = time.time()
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

            read_time_start = time.time()
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
            key_nm = nms[parser_data['column_numbers'][0]]
            if types[key_nm] not in ['bytes', 'str', 'object']:
                df_columns = {
                    nm: empty(info['row_num'], dtype=types[nm]) for nm in nms
                }
                key_pointer_str = {
                    'key': empty(info['row_num'], dtype=types[key_nm]),
                    'pointer': empty(info['row_num'], dtype='int32')
                }
            else:
                df_columns = {nm: empty(0, dtype=types[nm])
                              for nm in nms}
                key_pointer_str = {
                    'key': empty(0, dtype=types[key_nm]),
                    'pointer': empty(0, dtype='int32')
                }

            nms_without_key = nms.copy()
            nms_without_key.remove(key_nm)

            current_lower_bound = 0
            retry_count=0

            while read_cnks < len(parts_info):
                try:

                    chunk_name = q_writes.get(block=True, timeout=QUEUE_TIMEOUT)

                    with open(chunk_name, 'rb') as f:
                        df = pickle.load(f)

                    current_shape = df.shape[0]

                    print("Read {} rows from {}".format(current_shape, chunk_name))

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

            del key_pointer_str['pointer']

            if parser_data['low_memory']:
                with open("keys", "rb") as f:
                    key_pointer_str = {'key': pickle.load(f)}

            df = _create_dataframe(df_columns, key_pointer_str['key'], types, key_nm)

            # return df, sort_times
            return df

        info = {'row_num': row_num, 'total_num': len(parts_info)}
        fts = []
        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer, info)
            producer_ft = pool.submit(_chunk_producer, 0)
            ed = consumer_thread_future.result()

        # return -1
        after_readt = time.time()
        print("DEV:::REDUCE SEGMENT; {} reads - {} MB read - {} s time".format(len(parts_info),
                                                                                           sum(list(
                                                                                               sizeq.queue)) / (
                                                                                                       1024 ** 2),
                                                                                           after_readt - before_readt
                                                                                           ))

        total_size = ed.shape[0]

        if total_size == 0:
            print("Segment {} is void".format(segm_i))
            ret_dict['correctness'] = ret_dict['correctness'] & True

        else:

            print("Segment shape after {}".format(ed.shape))

            # Pd
            num_rows = total_size
            mem = ed.memory_usage(index=False)
            final_segment_size_bytes = sum([mem[i] for i in range(len(ed.columns))])
            avg_bytes_per_row = final_segment_size_bytes / num_rows

            print("Row size {}; total segment size {}; minimum upload size {}".format(avg_bytes_per_row,
                                                                                      final_segment_size_bytes,
                                                                                      MIN_MULTIPART_SIZE * GRANULE_SIZE_TO_BYTES ))

            if parser_data['low_memory']:
                _multipart_upload(ibm_cos, output_path, output_bucket, ed, DEFAULT_MULTIPART_NUMBER, segm_i)
            else:
                ibm_cos.put_object(Bucket=output_bucket,
                                   Key="{}/{}.pickle".format(output_path, segm_i),
                                   Body=pickle.dumps(ed))

    return time.time()-start_time

    return total_size