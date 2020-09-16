import os
import pickle
import re
from concurrent.futures.thread import ThreadPoolExecutor
import time
from random import randrange, seed

from ibm_botocore.exceptions import ClientError
from pandas import read_csv, DataFrame
from queue import Queue as threadPoolQueue
import numpy as np
from numpy import empty, array, argsort, searchsorted, concatenate, take, full, iinfo, finfo, dtype
from pywren_ibm_cloud.sort.monitor.config import SHUFFLE_OUTPUT_PREFIX
from pywren_ibm_cloud.sort.monitor.patch import patch_map_monitor, unpatch_map_monitor
from pywren_ibm_cloud.sort.monitor.task_progress_communication import _upload_progress, _check_end_signal, \
    _check_if_speculative
# Basic sort comparisons
# from pywren_ibm_cloud.sort.tests.basic_sort import _partition_into_segments_basic
# Granulated vs ungranulated read throughput comparison
from pywren_ibm_cloud.sort.tests.granulated_performance.ungranulated_read import _partition_into_segments_basic

from pywren_ibm_cloud.sort.utils import _extract_real_bounds_csv, get_normalized_type_names, correct_types
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE, DEFAULT_GRANULE_SIZE_REDUCE, \
    GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION, DEFAULT_THREAD_NUM, DATAFRAME_ALLOCATION_MARGIN


# No monitor option
# _partition_into_segments_standalone
def _partition_into_segments_standalone(args, ibm_cos):
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
    print("Got segment info from COS *********************")
    print("{}".format(segment_info))
    # Granuled upload
    total_bytes = upper_bound - lower_bound
    print("Will sort {} MB of data".format(total_bytes / (1024 ** 2)))
    chunk_size = int(granule_size * (1024 ** 2))
    read_bounds = list(range(lower_bound, upper_bound, chunk_size))
    read_bounds.append(upper_bound)
    for i in range(len(read_bounds) - 1):
        lb, ub = _extract_real_bounds_csv(ibm_cos, read_bounds[i], read_bounds[i + 1], total_size, input_bucket,
                                          input_path)
        read_bounds[i] = lb
    print("Reading in {} chunks".format(len(read_bounds) - 1))
    print(read_bounds)

    # seed()
    # straggler_iteration = randrange(DEFAULT_THREAD_NUM - 1)
    print("reading from bucket {} and file {}".format(input_bucket, input_path))

    read_0 = time.time()
    for c_i in range(len(read_bounds) - 1):
        print("Reading parallel chunk {}".format(c_i))
        read_part = ibm_cos.get_object(Bucket=input_bucket,
                                       Key=input_path,
                                       Range=''.join(
                                           ['bytes=', str(read_bounds[c_i]), '-',
                                            str(read_bounds[c_i + 1] - 1)]))

        with open("aux_{}".format(c_i), "wb") as f:
            f.write(read_part['Body'].read())

    read_1 = time.time()

    return read_1-read_0
    ####################