import csv
import pickle
from io import StringIO
from math import ceil
from random import sample
import numpy as np
import pandas as pd
from pywren_ibm_cloud.sort.fragment import fragment_dataset_into_chunks_csv

from pywren_ibm_cloud.sort.utils import _extract_real_bounds_csv, BOUND_EXTRACTION_MARGIN
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE, SAMPLE_RATIO, CANARY_PREDICTOR_RATIO, \
    MAX_READS_PER_SAMPLER, SAMPLE_RANGE_FILE_NAME


BUCKET_NAMES = [ "german-{}".format(i) for i in range(25) ]


def _parse(info, ibm_cos):

    print("Generate byte ranges")
    input_path = info['input_path']
    parser_file_path = info['parser_file_path']
    num_workers_phase1 = info['num_workers_phase1']
    chunk_range_bucket = info['chunk_range_bucket']
    chunk_range_file_path = info['chunk_range_file_path']
    input_bucket = info['input_bucket']
    output_bucket = info['output_bucket']
    delimiter = info['delimiter']



    #############################
    info['bucket_names'] = BUCKET_NAMES
    input_bucket = "german-0"
    input_path = "{}_0".format(input_path)
    #############################

    partial_size = ibm_cos.get_object(Bucket=input_bucket, Key=input_path)['ContentLength']
    total_size = partial_size*len(BUCKET_NAMES)
    info['total_size'] = total_size
    info['low_memory'] = True

    partition_file_path = info['partition_file_path']

    # Infer approximate total rows
    c_b, c_u = _extract_real_bounds_csv(ibm_cos, 0, BOUND_EXTRACTION_MARGIN * 5, partial_size, input_bucket, input_path)
    c = ibm_cos.get_object(Bucket=input_bucket,
                           Key=input_path,
                           Range=''.join(
                               ['bytes=', str(c_b), '-',
                                str(c_u)]))['Body'].read()

    # Infer delimiter if necessary.
    if info['delimiter'] is None:
        delimiter = csv.Sniffer().sniff(c.decode('utf-8'), delimiters=';,:').delimiter
        info['delimiter'] = delimiter

    test_df = StringIO(c.decode('utf-8')+'\n')
    df = pd.read_csv(test_df,  index_col=None, engine='c', header=None, delimiter=info['delimiter'])
    bytes_per_row = (int(c_u)-int(c_b)) / df.shape[0]
    info['total_rows_approx'] = int(total_size / bytes_per_row)
    print("delimiter <{}>".format(delimiter))
    print("total size {}".format(total_size))

    if info['sample'] or info['canary']:
        sampling_info = dict()

    # Define chunks for the canary predictor.
    if info['canary']:
        chunk_size_per_canary = int((total_size * CANARY_PREDICTOR_RATIO) / (MAX_READS_PER_SAMPLER))
        possible_chunks = range(0, total_size - chunk_size_per_canary, chunk_size_per_canary)
        selected_canary_chunks = sample(list(possible_chunks), MAX_READS_PER_SAMPLER)
        ibm_cos.put_object(Bucket=input_bucket,
                           # TODO: argument for canary output.
                           Key="tmp/segment_ranges/canary_ranges.pickle",
                           Body=pickle.dumps(selected_canary_chunks))
        sampling_info['chunk_size_per_canary'] = chunk_size_per_canary

    # Define reads per sample
    # TODO: Adaptatively calculate number of reads per sampler based on number of workers, bandwidth and throughput.
    chunk_size_per_sampler = int((total_size * SAMPLE_RATIO) / (MAX_READS_PER_SAMPLER * num_workers_phase1))
    virtual_workers = int(num_workers_phase1/len(BUCKET_NAMES))
    possible_chunks = range(0, partial_size, chunk_size_per_sampler)
    # TODO:
    # Divide possible chunks in MAX_READS_PER_SAMPLER groups
    sample_chunk_ranges = np.array_split(possible_chunks, MAX_READS_PER_SAMPLER)
    selected_sample_chunks = [sample(list(sample_chunk_ranges[i]), virtual_workers) for i in
                              range(MAX_READS_PER_SAMPLER)]
    global_ranges = [[s[i] for s in selected_sample_chunks] for i in range(virtual_workers)]
    sampling_info['sample_ranges'] = global_ranges
    sampling_info['chunk_size_per_sampler'] = chunk_size_per_sampler
    print(global_ranges)

    ibm_cos.put_object(Bucket=output_bucket,
                       Key="{}/{}.pickle".format(info['sample_info_path'], SAMPLE_RANGE_FILE_NAME),
                       Body=pickle.dumps(sampling_info))
    print("Sampling info uploaded")
    ibm_cos.put_object(Bucket=output_bucket,
                       Key=parser_file_path,
                       Body=pickle.dumps(info))
    print("Parser info uploaded into {}".format(parser_file_path))

    ###############################################################
    chunk_bounds = fragment_dataset_into_chunks_csv(int(num_workers_phase1/len(BUCKET_NAMES)), partial_size)
    print("chunk bounds {}".format(chunk_bounds))
    ###############################################################

    ibm_cos.put_object(Bucket=chunk_range_bucket,
                       Key=chunk_range_file_path,
                       Body=pickle.dumps(chunk_bounds))
    print("Chunk ranges uploaded")
    if info['segment_ranges'] is not None:
        ibm_cos.put_object(Bucket=output_bucket,
                           Key=partition_file_path,
                           Body=pickle.dumps(info['segment_ranges']))
        print("Partition file uploaded into {}".format(partition_file_path))


def parse(pw,
          input_bucket,
          output_bucket,
          input_path,
          intermediate_path,
          output_path,
          num_workers_phase1,
          num_workers_phase2,
          delimiter,
          granularity,
          partition_file_path,
          parser_file_path,
          sample_info_path,
          partition_bucket,
          chunk_range_file_path,
          chunk_range_bucket,
          sort_column_numbers,
          runtime_memory,
          dtypes=None,
          sample=False,
          sampling_mode="random",
          canary=False,
          segment_ranges=None):

    parse_params = {'info': {'input_bucket': input_bucket,
                             'output_bucket': output_bucket,
                             'input_path': input_path,
                             'intermediate_path': intermediate_path,
                             'output_path': output_path,
                             'num_workers_phase1': num_workers_phase1,
                             'num_workers_phase2': num_workers_phase2,
                             'delimiter': delimiter,
                             'granularity': granularity,
                             'partition_file_path': partition_file_path,
                             'parser_file_path': parser_file_path,
                             'sample_info_path': sample_info_path,
                             'partition_bucket': partition_bucket,
                             'chunk_range_file_path': chunk_range_file_path,
                             'chunk_range_bucket': chunk_range_bucket,
                             'column_numbers': sort_column_numbers,
                             'dtypes': dtypes,
                             'sample': sample,
                             'sampling_mode': sampling_mode,
                             'canary': canary,
                             'runtime_memory': runtime_memory,
                             'segment_ranges': segment_ranges}}

    futures = pw.call_async(_parse, parse_params)
    print("Parsing dataset")
    pw.wait(futures)