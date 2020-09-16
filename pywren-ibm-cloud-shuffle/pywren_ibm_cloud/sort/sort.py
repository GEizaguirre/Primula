import logging
import ntpath
import os
import pickle
import shutil
import time

import pywren_ibm_cloud.sort.config
from pywren_ibm_cloud.sort.asynchronous.sort import sort_async
from pywren_ibm_cloud.sort.config import INTERMEDIATE_PATH_PREFIX, OUTPUT_PATH_PREFIX, \
    DEFAULT_GRANULE_SIZE_SHUFFLE, PARTITION_FILE_PATH_PREFIX, CHUNK_RANGE_FILE_NAME, \
    PARSER_FATA_FILE_NAME, SAMPLE_PATH_PREFIX
from pywren_ibm_cloud.sort.parse import parse
from pywren_ibm_cloud.sort.phase1 import partitions_into_segments_csv
from pywren_ibm_cloud.sort.predict.search_optimal import search_optimal
from pywren_ibm_cloud.sort.tests.phase1_alternatives.phase1_imzml import map_chunks_into_segments_imzml_v3
from pywren_ibm_cloud.sort.phase2 import reduce_partition_segments
from pywren_ibm_cloud.sort.sample import sample_dataset_imzml, sample_dataset_csv

# Using enum class create enumerations
from pywren_ibm_cloud.sort.utils import get_dataset_size

SORT_DATA_FORMATS = {"imzml", "csv"}
DEFAULT_NUM_WORKERS = 5

logger = logging.getLogger(__name__)


# TODO: delete sleep time argument, only for straggler sensitivity testing.
def create_sort_csv(pw, input_data_path, format_of_data, num_segm, primary_key_column,
                    num_workers_p1=None, granularity=None, delimiter=None, dtypes=None,
                    speculative_map=False, speculative_reduce=False, asynchronous=False,
                    output_path=None, output_bucket=None, segment_ranges=None):
    print("Sort dataset in {} segments".format(num_segm))
    print("Key column: {}".format(primary_key_column))
    print("Asynchronous: {}".format(asynchronous))
    print("Speculative execution: map {} reduce {}".format(speculative_map, speculative_reduce))

    # Manage path format
    if input_data_path.startswith("cos"):
        bucket = input_data_path.split("/")
        bucket = bucket[3]
        path = '/'.join(input_data_path.split("/")[4:])
    else:
        path = input_data_path
    if segment_ranges is not None:
        if isinstance(segment_ranges, str):
            segment_ranges = pickle.load(open(segment_ranges, 'rb'))
            try:
                iter(segment_ranges)
            except Exception:
                raise ValueError("segment_ranges object must be iterable")
            segment_ranges = list(segment_ranges)
        if not isinstance(segment_ranges, list):
            raise ValueError("segment_ranges param must be a list")
    file_name = ntpath.basename(path)
    csv_file_path = path

    my_executor_id = pw.executor_id.replace("/", "_")
    intermediate_path = "{}_{}_{}".format(INTERMEDIATE_PATH_PREFIX, file_name, my_executor_id)
    if output_path is None:
        output_path = "{}_{}_{}".format(OUTPUT_PATH_PREFIX, file_name, my_executor_id)
    if output_bucket is None:
        output_bucket = bucket
    if num_workers_p1 is None:
        num_workers_p1 = num_segm
    if granularity is None:
        granularity = DEFAULT_GRANULE_SIZE_SHUFFLE
    partition_file_path = "{}_{}_{}".format(PARTITION_FILE_PATH_PREFIX, file_name, my_executor_id)
    chunk_range_file_path = "{}/{}.pickle".format(intermediate_path, CHUNK_RANGE_FILE_NAME)
    parser_file_path = "{}/{}.pickle".format(intermediate_path, PARSER_FATA_FILE_NAME)
    sample_info_path = "{}_{}_{}".format(SAMPLE_PATH_PREFIX, file_name, my_executor_id)


    if num_segm is None:
        dataset_size = get_dataset_size(pw, bucket, csv_file_path)
        print("Dataset size {}".format(dataset_size))
        num_segm = search_optimal(pw.config, dataset_size, granularity)
        num_workers_p1 = num_segm
        print("Inferenced {} mappers and {} reducers".format(num_workers_p1, num_segm))

    print("Parsing")
    parse(pw,
          input_bucket=bucket,
          output_bucket=bucket,
          input_path=csv_file_path,
          intermediate_path=intermediate_path,
          output_path=output_path,
          num_workers_phase1=num_workers_p1,
          num_workers_phase2=num_segm,
          delimiter=delimiter,
          granularity=granularity,
          partition_file_path=partition_file_path,
          parser_file_path=parser_file_path,
          sample_info_path=sample_info_path,
          partition_bucket=bucket,
          chunk_range_file_path=chunk_range_file_path,
          chunk_range_bucket=bucket,
          sort_column_numbers=[primary_key_column],
          runtime_memory=pw.config['pywren']['runtime_memory'],
          sample=True,
          dtypes=dtypes,
          sampling_mode="random",
          canary=False,
          segment_ranges=segment_ranges)

    if segment_ranges is None:
        print("Sampling")
        sample_dataset_csv(pw,
                           parser_file_path,
                           output_bucket,
                           num_workers_p1,
                           sample_type="random",
                           canary=False)

    print("Mapping chunks into segments")

    # Create straggler info
    # random.seed()
    # straggler_ids_p1 = random.sample(list(range(num_workers_p1)), max(int(num_workers_p1*DEFAULT_SPECULATIVECAP_TOTAL_TASKS), 1))
    # for id in straggler_ids_p1:
    #       ibm_cos.put_object(Bucket=bucket, Key="straggler_{}".format(id), Body=pickle.dumps(id))
    # print(straggler_ids_p1)
    # straggler_ids_p2 = random.sample(list(range(num_segm)),
    #                                   max(int(num_segm * DEFAULT_SPECULATIVECAP_TOTAL_TASKS), 1))
    # for id in straggler_ids_p2:
    #      ibm_cos.put_object(Bucket=bucket, Key="straggler_p2_{}".format(id), Body=pickle.dumps(id))
    # print(straggler_ids_p2)

    if not asynchronous:
        partitions_into_segments_csv(pw,
                                     num_workers_p1,
                                     parser_file_path,
                                     output_bucket,
                                     speculative=speculative_map)

        print("Reducing segments")
        reduce_partition_segments(pw,
                                  num_segm,
                                  parser_file_path,
                                  output_bucket,
                                  speculative=speculative_reduce)

    else:
        sort_async(pw,
                   num_workers_p1,
                   num_segm,
                   parser_file_path,
                   output_bucket,
                   speculative_map=speculative_map,
                   speculative_reduce=speculative_reduce)

    print("Sort finished, output segments at: ")
    print("bucket {}".format(output_bucket))
    print("path {}".format(output_path))


def create_sort_imzml(pw, input_data_path, format_of_data, ibm_cos, segm_n, num_workers=None, granularity=None):
    if granularity is not None:
        pywren_ibm_cloud.sort.config.DEFAULT_GRANULE_SIZE_SHUFFLE = granularity
        # ut.DEFAULT_GRANULE_SIZE_REDUCE = granularity

    print("Sort dataset in {} segments".format(segm_n))
    start_time = time.time()

    if num_workers != None:
        effective_num_workers = num_workers
    else:
        effective_num_workers = DEFAULT_NUM_WORKERS

    # Directory for temporary files
    shutil.rmtree('tmp', ignore_errors=True)
    os.mkdir("tmp")

    print("Generating parser")
    # Manage path format
    if input_data_path.startswith("cos"):
        bucket = input_data_path.split("/")

        bucket = bucket[3]
        path = '/'.join(input_data_path.split("/")[4:])
    else:
        path = input_data_path

    print("Bucket " + bucket + " and path " + path)

    prefix = "ordered_segments"
    objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while 'Contents' in objs:
        keys = [obj['Key'] for obj in objs['Contents']]
        formatted_keys = {'Objects': [{'Key': key} for key in keys]}
        ibm_cos.delete_objects(Bucket=bucket, Delete=formatted_keys)

        objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Create local metadata object
    # imzml_metadata = ibm_cos.get_object(Bucket=bucket, Key=''.join([path, ".imzml"]))['Body'].read()
    # metadata_file_path = "tmp/" + input_data_path.split("/")[-1] + ".imzml"
    ibd_file_path = ''.join([path, ".ibd"])
    metadata_file_path = ''.join([path, ".imzml"])

    num_workers, parser_data, start_sampling_time = sample_dataset_imzml(pw, bucket,
                                                                         metadata_file_path, ibd_file_path,
                                                                         effective_num_workers,
                                                                         "random", segm_n, ibm_cos)
    print("Sampling finished")
    print("Final number of workers {}".format(num_workers))
    #
    start_time_effective = time.time()
    print("Mapping chunks into segments")

    map_chunks_into_segments_imzml_v3(pw, num_workers, bucket, ibd_file_path,
                                      bucket, "tmp/partition_file.pickle", parser_data)

    print("Reducing segments")
    # reduce_chunk_segments_imzml(pw, num_workers, bucket, bucket,
    #                             segm_n)

    end_time_effective = time.time()

    # # Sample dataset
    print("Removing temporal files")

    shutil.rmtree('tmp', ignore_errors=True)
    prefix = "tmp"
    args = {'info': {'prefix': prefix, 'bucket': bucket}}
    futures = pw.call_async(ut.clear_cos_prefix, args)
    pw.get_result(futures)

    end_time = time.time()
    print("Sort ended")
    print("--- %s seconds ---" % (end_time - start_time))
    print("--- %s seconds without loading ---" % (end_time - start_sampling_time))
    print("--- %s seconds without loading and sampling ---" % (end_time - start_time_effective))
    print("--- %s seconds without final clearing ---" % (end_time_effective - start_time))

    f = open("equations_results.txt", 'a')
    f.write("{}\n".format([(end_time - start_time), (end_time - start_sampling_time), (end_time - start_time_effective),
                           (end_time_effective - start_time)]))
    f.close()
