import os
import pickle
import time
from concurrent.futures.thread import ThreadPoolExecutor

import numpy as np
import pandas as pd
from ibm_botocore.exceptions import ClientError
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.config import DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN, DEFAULT_GRANULE_SIZE_SHUFFLE, \
    GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION, MIN_MULTIPART_SIZE
from pywren_ibm_cloud.sort.utils import get_normalized_type_names, correct_types, _extract_real_bounds_csv
from queue import Queue as threadPoolQueue


def _partition_into_segments_basic(args, ibm_cos):
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
    print(parser_data)

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
    print("Got partition info from COS *********************")
    print("{}".format(segment_info))
    # Granuled upload
    total_bytes = upper_bound - lower_bound
    print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))

    sizel = []
    before_readt = time.time()

    # seed()
    # straggler_iteration = randrange(DEFAULT_THREAD_NUM - 1)
    print("reading from bucket {} and file {}".format(input_bucket, input_path))

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    normalized_type_names = get_normalized_type_names(types)
    nms = [str(i) for i in range(len(parser_data['dtypes']))]

    read_0 = time.time()

    read_part = ibm_cos.get_object(Bucket=input_bucket,
                                   Key=input_path,
                                   Range=''.join(
                                       ['bytes=', str(lower_bound), '-',
                                        str(upper_bound - 1)]))


    with open("aux", "wb") as f:
        f.write(read_part['Body'].read())

    read_1 = time.time()

    return read_1 - read_0
