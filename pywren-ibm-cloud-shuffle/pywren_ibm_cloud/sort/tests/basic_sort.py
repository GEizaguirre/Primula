import os
import pickle
import time
from concurrent.futures.thread import ThreadPoolExecutor

import numpy as np
import pandas as pd
from ibm_botocore.exceptions import ClientError
from pandas import cut, Series
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

    read_part = ibm_cos.get_object(Bucket=input_bucket,
                                   Key=input_path,
                                   Range=''.join(
                                       ['bytes=', str(lower_bound), '-',
                                        str(upper_bound - 1)]))

    # ed = pd.read_csv(read_part['Body'], index_col=None, header=None, names=nms,
    #                  delimiter=delimiter, dtype=types)
    ed = pd.read_csv(read_part['Body'])
    ed.columns = nms

    # # Adjust types
    # if not correct_types(ed, normalized_type_names):
    #     print("Adjusting types from")
    #     print(ed.dtypes)
    #     print("to")
    #     print(normalized_type_names)
    #     df_partial_columns = {nm: np.array(list(ed[nm]), dtype=types[nm]) for nm in nms}
    #     del ed
    #     nm = nms[0]
    #     ed = pd.DataFrame(df_partial_columns[nm])
    #     for nm in nms[1:]:
    #         ed[nm] = df_partial_columns[nm]
    #     ed.columns = nms

    key_series = ed['1']
    bins = segment_info
    indexes = Series(list(range(len(segment_info) - 1)))
    # Classify rows into segments.
    classified_indexes = cut(key_series, bins, labels=indexes, include_lowest=True).values.add_categories(
        len(segment_info) - 1)
    classified_indexes = np.array(classified_indexes.fillna(len(segment_info) - 1))

    segments = []
    total_rows_after = 0
    for i in range(len(segment_info)):
        # extract segments' rows
        selected_rows_indexes = list(np.where(classified_indexes == i)[0])
        segments.append(ed.loc[selected_rows_indexes])
        total_rows_after += len(ed.loc[selected_rows_indexes])

    print("rows to upload: {}".format(ed.shape[0]))

    def _order_upload_segm_info(segm_i):
        print("writing {}/sgm{}/gran{}.pickle".format(my_output_path, segm_i, 1))
        print("{} rows".format(len(segments[segm_i])))
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/sgm{}/gran{}.pickle".format(my_output_path, segm_i, 1),
                           Body=pickle.dumps(segments[segm_i]))

    [ _order_upload_segm_info(i) for i in range(parser_data['num_workers_phase2']) ]

    return total_rows_after


def _reduce_partitions_basic(args, ibm_cos):

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

    types = dict()
    for i in range(len(parser_data['dtypes'])):
        types[str(i)] = parser_data['dtypes'][i]
    nms = [str(i) for i in range(len(parser_data['dtypes']))]

    for segm_i in segms:

        print("Reducing segment {}".format(segm_i))

        def _extract_range_segment (partition_i):
            intmd_path = "{}/{}/sgm{}/gran{}.pickle".format(intermediate_path, partition_i, segm_i, 1)
            print("reading {}".format(intmd_path))
            read_part = ibm_cos.get_object(Bucket=output_bucket,
                                           Key=intmd_path)
            dfp = pickle.loads(read_part['Body'].read())
            print("read {} rows".format(dfp.shape[0]))
            return dfp

        before_readt = time.time()

        obj = _extract_range_segment(0)
        for i in range(1, partition_number):
            obj = obj.append(_extract_range_segment(i))

        # return -1
        after_readt = time.time()
        read_time += after_readt - before_readt
        print("partitions read at {}".format(after_readt-before_readt))

        upload_done = False
        # Pd
        total_size = obj.shape[0]
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

        ibm_cos.put_object(Bucket=output_bucket,
                           Key="{}/{}.pickle".format(output_path, segm_i),
                           Body=pickle.dumps(obj))

    return total_size
