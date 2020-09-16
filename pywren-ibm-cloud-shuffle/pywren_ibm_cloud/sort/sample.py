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


def _spectra_sample_gen(imzml_parser, range_values, sample_ratio=0.05):
    sp_n = len(imzml_parser.coordinates)
    # print(f'sp_n : {sp_n}')
    sample_size = int(sp_n * sample_ratio)
    # print(f'original sample_size : {sample_size}')
    sample_size = math.floor(sample_size / len(range_values))
    # print(f'our sample_size : {sample_size}')

    num = 0
    for ranges in range_values:
        # print(f'Sampling range {num}')
        num += 1
        sample_sp_inds = choice(arange(len(ranges[0])), sample_size)
        for sp_idx in sample_sp_inds:
            # print(f'Sampling index {sp_idx}')

            imzml_parser.m.seek(ranges[0][sp_idx][0])
            mzs = imzml_parser.m.read(ranges[1][sp_idx][0])
            # print(f'Reading size {ranges[1][sp_idx][0]}')
            imzml_parser.m.seek(ranges[0][sp_idx][1])
            ints = imzml_parser.m.read(ranges[1][sp_idx][1])
            mz_array = frombuffer(mzs, dtype=imzml_parser.mzPrecision)
            intensity_array = frombuffer(ints, dtype=imzml_parser.intensityPrecision)
            # print(f'Has {len(mz_array)} values')
            yield sp_idx, mz_array, intensity_array


def _byte_range_gen_imzml_bounds(ranges, bound_list, ibm_cos, bucket):
    # range_values = list()
    for rng in range(0, len(bound_list) - 1):
        length_list = list()
        offset_list = list()
        for i in range(bound_list[rng], bound_list[rng + 1]):
            offsets = [ranges[i][0], ranges[i][1]]
            lengths = [ranges[i][2],
                       ranges[i][3]]
            offset_list.append(offsets)
            length_list.append(lengths)
        range_values = [offset_list, length_list, rng, bound_list[rng], bound_list[rng + 1]]
        print("chunk {} range [{} - {}], total {} coordinates".format(rng, bound_list[rng], bound_list[rng + 1],
                                                                      bound_list[rng + 1] - bound_list[rng]))
        ibm_cos.put_object(Bucket=bucket,
                           Key="tmp/chunk_ranges/rng{}.pickle".format(rng),
                           Body=pickle.dumps(range_values))

    return range_values


def _sample_dataset_izml_random(pw, parser_data, bucket,
                                ibd_file_path, num_workers,
                                segm_n):
    """
       This method does a random sampling over an ibd file byte range.

       :param range:
           Range string in HTTP range format.

       :return d:
           blablabla
       :rtype:
           blablabla
       :raises Warning:
           blablabla
       """
    # Random sampling
    sample_ratio = SAMPLE_RATIO
    sp_n = int(parser_data['sp_n'])
    sample_total_size = int(sp_n * sample_ratio)
    mz_precision = parser_data['mzPrecision']
    int_precision = parser_data['intensityPrecision']

    # intensity_precision = imzml_parser.intensityPrecision

    def _partial_partition_file_gen_imzml(num_range, memory_size, ibm_cos):
        print("++++++++++++++++++++++++++")
        print(f'Range {num_range}')
        print(f'bucket_name {bucket}')
        print(f'file_name {ibd_file_path}')
        print('total memory {}'.format(memory_size))
        print("++++++++++++++++++++++++++")

        range_values = pickle.loads(
            ibm_cos.get_object(Bucket=bucket, Key="tmp/sampling_ranges/sample_range{}.pickle".format(num_range))[
                'Body'].read())

        # part_file_name = "tmp/part{}.pickle".format(num_range)

        cos_wrapper = CosFileBinaryWrapper(ibm_cos, bucket, ibd_file_path)
        offsets = range_values[0]
        lengths = range_values[1]
        sample_size = math.floor(sample_total_size / num_workers)
        sample_sp_inds = choice(arange(len(range_values[0])), sample_size)
        average_size_p_coord = 0
        first = True
        for sp_idx in sample_sp_inds:

            cos_wrapper.seek(offsets[sp_idx][0])
            mzs = cos_wrapper.read(lengths[sp_idx][0])

            # cos_wrapper.seek(offsets[sp_idx][1])
            # ints = cos_wrapper.read(lengths[sp_idx][1])

            mz_array = frombuffer(mzs, dtype=mz_precision)

            # Byte conversion to kilobytes
            average_size_p_coord += mz_array.nbytes / (2 ** 10)
            # intensity_array = frombuffer(ints, dtype=intensity_precision)
            if first:
                spectra_mzs = mz_array
            else:
                spectra_mzs = concatenate(spectra_mzs, mz_array)
                first = False

        sort(spectra_mzs)
        segm_bounds_q = [i * 1 / segm_n for i in range(1, segm_n)]
        segm_lower_bounds = [quantile(spectra_mzs, q) for q in segm_bounds_q]

        average_size_p_coord = average_size_p_coord * 3 / len(sample_sp_inds)
        # Calculate in megabytes
        average_size_p_coord = average_size_p_coord / (2 ** 10)
        coord_p_chunk = int((SAMPLE_RUNTIME_MEMORY * MAP_AVAILABLE_MEMORY_RATE) / average_size_p_coord)

        print("Partial coordinates per chunk {}".format(coord_p_chunk))

        partial_partition_info = dict()
        partial_partition_info['segments'] = segm_lower_bounds
        partial_partition_info['coord_p_chunk'] = coord_p_chunk

        ibm_cos.delete_object(Bucket=bucket, Key="tmp/sampling_ranges/sample_range{}.pickle".format(num_range))
        ibm_cos.put_object(Bucket=bucket,
                           Key="tmp/partial_partition_file{}.pickle".format(num_range),
                           Body=pickle.dumps(partial_partition_info))
        print("Partial partitions uploaded")
        return num_range

    def _reduce_bounds(results, ibm_cos):
        partial_file_name = "tmp/partial_partition_file{}.pickle".format(results[0])
        partial_info = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key=partial_file_name)['Body'].read())
        for key, value in partial_info.items():
            print(key)
        array1 = partial_info[b'segments']
        acum_coord_p_chunk = partial_info[b'coord_p_chunk']
        ibm_cos.delete_object(Bucket=bucket, Key=partial_file_name)
        print("Partial bounds{}".format(results[0]))
        for bound in array1:
            print(bound)
        for i in range(1, len(results)):
            partial_file_name = "tmp/partial_partition_file{}.pickle".format(results[i])
            partial_info = pickle.loads(ibm_cos.get_object(Bucket=bucket, Key=partial_file_name)['Body'].read())
            array2 = partial_info[b'segments']
            acum_coord_p_chunk += partial_info[b'coord_p_chunk']
            print("Partial bounds{}".format(results[i]))
            for bound in array2:
                print(bound)
            for pos in range(len(array1)):
                array1[pos] = (array1[pos] + array2[pos]) / 2
            ibm_cos.delete_object(Bucket=bucket, Key=partial_file_name)

        array1 = insert(array1, 0, 0.0, axis=0)
        array1 = insert(array1, len(array1), MAX_MSZ_VALUE, axis=0)

        print("Final segment bounds")
        for bound in array1:
            print(bound)

        print("acum_coord_p_chunk {}".format(acum_coord_p_chunk))
        print("Results length {}".format(len(results)))
        avg_coord_p_chunk = int(acum_coord_p_chunk / len(results))
        print("Coordinates per chunk {}".format(avg_coord_p_chunk))

        ibm_cos.put_object(Bucket=bucket,
                           Key="tmp/partition_file.pickle",
                           Body=pickle.dumps(array1))
        # ibm_cos.put_object(Bucket=bucket,
        #                    Key="tmp/chunk_size.msgpack",
        #                    Body=msgpack.dumps(avg_coord_p_chunk))

        # chunk_bounds = fragment_dataset_into_chunks_imzml(num_workers, sp_n, cnk_size=avg_coord_p_chunk)
        chunk_bounds = fragment_dataset_into_chunks_imzml(num_workers, sp_n)
        global_ranges = pickle.loads(ibm_cos.get_object(Bucket=bucket,
                                                        Key="tmp/segment_ranges/global_ranges.pickle")['Body'].read())
        _byte_range_gen_imzml_bounds(global_ranges, chunk_bounds, ibm_cos, bucket)
        ibm_cos.delete_object(Bucket=bucket,
                              Key="tmp/segment_ranges/global_ranges.pickle")
        # Generate parser and readjust ranges
        return len(chunk_bounds) - 1

    # TODO
    # Map the sampler related to sample_type against as many ranges of bytes as workers.
    # NOTE: By now, number of workers will be received as argument, we will not spend time on number
    # of worker calculation logic.
    # Reduce the sampling by reading the partial partition file of each sample mapper and
    # joining into a unique partition file.
    # Joining is performed by calculating the average of each correlated pair of segment
    # bounds.

    # Adjust reduce memory to imzml file size
    futures = pw.map_reduce(_partial_partition_file_gen_imzml, range(num_workers), _reduce_bounds,
                            extra_params=[SAMPLE_RUNTIME_MEMORY], map_runtime_memory=SAMPLE_RUNTIME_MEMORY)
    return format(pw.get_result(futures))


# TODO correct argument read from COS
def _canary_predictor_csv(ibm_cos, info):
    range_values = pickle.loads(
        ibm_cos.get_object(Bucket=info['bucket'], Key="tmp/segment_ranges/canary_ranges.pickle")['Body'].read())

    sampler_bounds = [
        _extract_real_bounds_csv(ibm_cos, bnd, bnd + info['parser_data']['chunk_size_per_canary'],
                                 info['parser_data']['total_size'], info['bucket'], info['csv_file_path'])
        for bnd in range_values]

    types = dict()
    for i in range(len(info['parser_data']['dtypes'])):
        types[str(i)] = info['parser_data']['dtypes'][i]
    nms = [str(i) for i in range(len(info['parser_data']['dtypes']))]

    q = queue.Queue(len(sampler_bounds))

    def _chunk_producer(i):
        read_part = ibm_cos.get_object(Bucket=info['bucket'],
                                       Key=info['csv_file_path'],
                                       Range=''.join(
                                           ['bytes=', str(sampler_bounds[i][0]), '-',
                                            str(sampler_bounds[i][1])]))
        with open("cnry_{}".format(i), 'wb') as f:
            f.write(read_part['Body'].read())

        q.put("cnry_{}".format(i))

    def _chunk_consumer():
        read_cnks = 0
        df_chunks = []
        canary_time = 0
        while True:
            if not q.empty():
                chunk_name = q.get()

                partial_time_s = time.time()
                df = read_csv(chunk_name, index_col=None, engine='c', header=None,
                              names=nms,
                              delimiter=info['parser_data']['delimiter'], dtype=types)

                df_chunks.append(df)

                partial_time_e = time.time()

                canary_time = canary_time + (partial_time_e - partial_time_s)

                read_cnks += 1
                if read_cnks == len(sampler_bounds):
                    canary_input = concat(df_chunks, ignore_index=True)
                    return canary_input, canary_time

            else:
                time.sleep(0.0001)

    with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
        consumer_thread_future = pool.submit(_chunk_consumer)
        for i in range(len(sampler_bounds)):
            pool.submit(_chunk_producer, i)
        canary_df, canary_time_r = consumer_thread_future.result()

    canary_time_start = time.time()

    target_array = canary_df[nms[info['parser_data']['column_numbers'][0]]]
    sorted_indexes = argsort(target_array)
    canary_df = canary_df.iloc[sorted_indexes]

    canary_time_end = time.time()

    return canary_time_r + (canary_time_end - canary_time_start)


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
        range_values = sampling_info['sample_ranges'][num_range]
        sampler_bounds = [
            _extract_real_bounds_csv(ibm_cos, bnd, bnd + chunk_size_per_sampler, total_size, bucket, input_path)
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

            read_part = ibm_cos.get_object(Bucket=bucket,
                                           Key=input_path,
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
            if parser_data['dtypes'][parser_data['column_numbers'][0]] == 'str':
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


def sample_dataset_imzml(pw, bucket, metadata_file_path,
                         ibd_file_path, num_workers, sample_type, segm_n, ibm_cos):
    byte_range_params = {'info': {'metadata_file_path': metadata_file_path,
                                  'ibd_file_path': ibd_file_path,
                                  'num_workers': num_workers,
                                  'bucket': bucket}}

    # byte_range_params = { 'metadata_file_path': metadata_file_path,
    #                      'ibd_file_path': ibd_file_path,
    #                      'num_workers': num_workers,
    #                      'bucket': bucket }

    # futures = pw.call_async(_byte_range_gen_imzml, byte_range_params, include_modules=['pyimzml'])
    futures = pw.call_async(_gen_byte_ranges_imzml, byte_range_params)
    print("Definition of chunk ranges")
    # parser_data = _byte_range_gen_imzml(byte_range_params, ibm_cos)

    # _byte_range_gen_imzml(imzml_parser, num_workers, bucket, ibm_cos)
    parser_data = pw.get_result(futures)
    start_sampling_time = time.time()

    print("Worker byte ranges defined")

    if sample_type == "random":
        return _sample_dataset_izml_random(pw, parser_data, bucket,
                                           ibd_file_path, num_workers,
                                           segm_n), parser_data, start_sampling_time


def sample_dataset_csv(pw, parser_info_path, parser_info_bucket, num_samplers, sample_type="random", canary=False):
    start_sampling_time = time.time()
    #
    if sample_type == "random":
        if canary:
            cnry_future = pw.call_async(_canary_predictor_csv, parser_info_path)

        _sample_dataset_csv_random(pw, parser_info_path, parser_info_bucket, num_samplers)
        #     print(sample_result)
        #     num_workers = sample_result['num_workers']
        #     parser_data['total_rows_approx'] = sample_result['total_rows_approx']
        #     print("Total_rows_approx {}".format(parser_data['total_rows_approx']))
        if canary:
            pw.wait(cnry_future)
    #     return num_workers, parser_data, canary_time
