import logging
import pickle
import queue
import re
from concurrent.futures.thread import ThreadPoolExecutor
from importlib import reload

from pywren_ibm_cloud.sort.CosFileBinaryWrapper import CosFileBinaryWrapper

import math

from pywren_ibm_cloud.sort.fragment import fragment_dataset_into_chunks_csv, fragment_dataset_into_chunks_imzml
from pywren_ibm_cloud.sort.utils import _extract_real_bounds_csv, _gen_byte_ranges_imzml
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE, PAR_LEVEL_LIMIT_PER_FUNCTION, \
    SAMPLE_RUNTIME_MEMORY, MAP_AVAILABLE_MEMORY_RATE, MAX_MSZ_VALUE, SAMPLE_RATIO, DEFAULT_THREAD_NUM, \
    SAMPLE_CHUNK_SIZE, SAMPLE_RANGE_FILE_NAME

import time
from pandas import read_csv, concat
from numpy import array, argsort, frombuffer, sort, arange, concatenate, insert, quantile, amax, amin
from numpy.random import choice

logger = logging.getLogger(__name__)

SAMPLE_TYPES = {"random", "interval", "split"}

def _sample_dataset_csv_random(pw, parser_info_path, parser_info_bucket, num_samplers):
    # Random sampling
    sample_ratio = SAMPLE_RATIO
    parser_info_dict = {'bucket': parser_info_bucket,
                        'path': parser_info_path}

    def _partial_partition_file_gen_csv(num_range, parser_info, ibm_cos):

        parser_data = pickle.loads(ibm_cos.get_object(Bucket=parser_info['bucket'],
                                                      Key=parser_info['path'])['Body'].read())
        sample_info_path = parser_data['sample_info_path']
        bucket = parser_data['output_bucket']
        sampling_info_key = "{}/{}.pickle".format(sample_info_path, SAMPLE_RANGE_FILE_NAME)
        cos_object = ibm_cos.get_object(Bucket=bucket, Key=sampling_info_key)
        sampling_info = pickle.loads(cos_object['Body'].read())
        input_path = parser_data['input_path']
        delimiter = parser_data['delimiter']
        segm_n = parser_data['num_workers_phase2']
        column_numbers = parser_data['column_numbers']
        total_size = parser_data['total_size']
        chunk_size_per_sampler = sampling_info['chunk_size_per_sampler']


        #######################################################
        num_buckets = len(parser_data['bucket_names'])
        bucket_list = parser_data['bucket_names']
        num_workers_phase1 = parser_data['num_workers_phase1']
        workers_p_bucket = num_workers_phase1 / num_buckets
        my_bucket = bucket_list[int(num_range / workers_p_bucket)]
        my_path = "{}_{}".format(input_path, int(num_range / workers_p_bucket))
        print("my_bucket: {}, my_path: {}".format(my_bucket, my_path))
        #######################################################

        range_values = sampling_info['sample_ranges'][int(num_range % workers_p_bucket)]
        sampler_bounds = [
            _extract_real_bounds_csv(ibm_cos, bnd, bnd + chunk_size_per_sampler, total_size, my_bucket, my_path)
            for bnd in range_values]
        # Granuled upload
        total_bytes = sum([b[1] - b[0] for b in sampler_bounds])
        print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))
        types = dict()
        for i in range(len(parser_data['dtypes'])):
            types[str(i)] = parser_data['dtypes'][i]
        nms = [str(i) for i in range(len(parser_data['dtypes']))]
        q = queue.Queue(len(sampler_bounds))

        def _chunk_producer(i):

            read_part = ibm_cos.get_object(Bucket=my_bucket,
                                           Key=my_path,
                                           Range=''.join(
                                               ['bytes=', str(sampler_bounds[i][0]), '-',
                                                str(sampler_bounds[i][1])]))
            part_size = read_part['ContentLength']
            # print("Reading parallel chunk {} of size {} MB".format(i, part_size / (1024 ** 2)))
            with open("c_{}".format(i), 'wb') as f:
                f.write(read_part['Body'].read())

            q.put("c_{}".format(i))

        def _chunk_consumer():

            target_array_chunks = []
            read_cnks = 0
            dataframe_rows = 0
            while True:
                if not q.empty():
                    chunk_name = q.get()
                    df = read_csv(chunk_name, index_col=None, engine='c', header=None,
                                  names=nms,
                                  delimiter=delimiter, dtype=types)

                    dataframe_rows += df.shape[0]
                    target_array_chunks.append(df[nms[column_numbers[0]]])

                    read_cnks += 1
                    if read_cnks == len(sampler_bounds):
                        bytes_per_row = dataframe_rows / total_bytes
                        return concatenate(target_array_chunks), bytes_per_row
                else:
                    time.sleep(0.0001)

        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
            consumer_thread_future = pool.submit(_chunk_consumer)
            for i in range(len(sampler_bounds)):
                pool.submit(_chunk_producer, i)
            target_array, bytes_per_row = consumer_thread_future.result()

        print(target_array)
        sort_indexes = argsort(target_array)
        sorted_array = target_array[sort_indexes]
        print(sorted_array)
        segm_bounds_q = [i * 1 / segm_n for i in range(1, segm_n)]
        if types[nms[column_numbers[0]]] not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is None:
            segm_lower_bounds = [quantile(sorted_array, q) for q in segm_bounds_q]
        else:
            segm_lower_bounds = [sorted_array[int(q*len(sorted_array))] for q in segm_bounds_q]

        partial_partition_info = dict()
        partial_partition_info[b'segments'] = segm_lower_bounds
        partial_partition_info[b'bytes_per_row'] = bytes_per_row
        ibm_cos.put_object(Bucket=bucket,
                           Key="{}/{}.pickle".format(sample_info_path, num_range),
                           Body=pickle.dumps(partial_partition_info))
        return num_range

    def _reduce_bounds(results, ibm_cos):

        parser_data = pickle.loads(ibm_cos.get_object(Bucket=parser_info_dict['bucket'],
                                                      Key=parser_info_dict['path'])['Body'].read())
        bucket = parser_data['output_bucket']
        sample_info_path = parser_data['sample_info_path']
        partition_file_path = parser_data['partition_file_path']
        total_size = parser_data['total_size']
        parser_file_path = parser_data['parser_file_path']
        partial_file_name = "{}/{}.pickle".format(sample_info_path, results[0])
        partial_info = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key=partial_file_name)['Body'].read())
        if parser_data['dtypes'][parser_data['column_numbers'][0]] not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is None:
            array1 = partial_info[b'segments']
        else:
            array1 = [ [bound] for bound in partial_info[b'segments'] ]
        ibm_cos.delete_object(Bucket=bucket, Key=partial_file_name)
        bytes_per_row = 0
        print("Partial bounds{}".format(results[0]))
        for i in range(1, len(results)):
            partial_file_name = "{}/{}.pickle".format(sample_info_path, results[i])
            partial_info = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key=partial_file_name)['Body'].read())
            array2 = partial_info[b'segments']
            bytes_per_row += partial_info[b'bytes_per_row']
            print("Partial bounds{}".format(results[i]))
            for pos in range(len(array1)):
                if parser_data['dtypes'][parser_data['column_numbers'][0]] not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is None:
                    # arithmetic average
                    array1[pos] = (array1[pos] + array2[pos]) / 2
                else:
                    # median
                    array1[pos].append(array2[pos])
            ibm_cos.delete_object(Bucket=bucket, Key=partial_file_name)

        if parser_data['dtypes'][parser_data['column_numbers'][0]] in ['bytes', 'str', 'object'] or \
                re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is not None:
            # median
            array1 = [ array1[pos][int(0.5*len(array1[pos]))] for pos in range(len(array1))]

        bytes_per_row = bytes_per_row / (len(results) - 1)
        total_rows_approx = int(total_size/bytes_per_row)
        if parser_data['dtypes'][parser_data['column_numbers'][0]] not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is None:
            array1 = insert(array1, 0, 0.0, axis=0)
        else:
            if parser_data['dtypes'][parser_data['column_numbers'][0]] is 'str':
                array1 = insert(array1, 0, bytes(bytearray([0])).decode('utf-8'), axis=0)
            else:
                if re.match("\|S[0-9]*", parser_data['dtypes'][parser_data['column_numbers'][0]]) is not None:
                    str_size = int(parser_data['dtypes'][parser_data['column_numbers'][0]][2:])
                    array1 = insert(array1, 0, bytes(bytearray([0 for i in range(str_size)])).decode('utf-8'), axis=0)
                else:
                    array1 = insert(array1, 0, bytes(bytearray([0])), axis=0)
        print("Final bounds")
        print(array1)
        print("Results length {}".format(len(results)))
        ibm_cos.put_object(Bucket=bucket,
                           Key="{}".format(partition_file_path),
                           Body=pickle.dumps(array1))
        sampling_info_key = "{}/{}.pickle".format(sample_info_path, SAMPLE_RANGE_FILE_NAME)
        ibm_cos.delete_object(Bucket=bucket,
                              Key=sampling_info_key)
        # Update parser data with more precise aproximate row number.
        parser_data['total_rows_approx'] = total_rows_approx
        ibm_cos.put_object(Bucket=bucket,
                           Key=parser_file_path,
                           Body=pickle.dumps(parser_data))
        return array1

    # Adjust reduce memory to imzml file size
    print("Generating {} samplers".format(num_samplers))
    futures = pw.map_reduce(_partial_partition_file_gen_csv, range(num_samplers), _reduce_bounds,
                            extra_params=[parser_info_dict])
    pw.wait(futures)

def sample_dataset_csv(pw, parser_info_path, parser_info_bucket, num_samplers, sample_type="random", canary=False):
    start_sampling_time = time.time()
    #
    if sample_type == "random":

        _sample_dataset_csv_random(pw, parser_info_path, parser_info_bucket, num_samplers)
