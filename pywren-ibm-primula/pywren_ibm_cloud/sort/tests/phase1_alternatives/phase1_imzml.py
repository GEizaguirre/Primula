import pickle
import threading
from concurrent.futures.thread import ThreadPoolExecutor
import time
from random import randrange, seed

import gevent
import gevent.monkey
from ibm_botocore.exceptions import ClientError
from pandas import read_csv, DataFrame, concat, merge
from queue import Queue as threadPoolQueue
from gevent.queue import Queue as greenletsQueue
from numpy import amin, amax, int32, empty, array, argsort, searchsorted, frombuffer, ones_like, around, concatenate
from pywren_ibm_cloud.sort.CosFileBinaryWrapper import CosFileBinaryWrapper
from pywren_ibm_cloud.sort.map_utils import get_pixel_indices
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE, DEFAULT_GRANULE_SIZE_REDUCE, \
    GRANULE_SIZE_TO_BYTES, PAR_LEVEL_LIMIT_PER_FUNCTION, MIN_MULTIPART_SIZE, DEFAULT_THREAD_NUM


def map_chunks_into_segments_imzml_v3(pw, num_workers, dataset_bucket, dataset_file_path,
                                      segments_bucket, partition_file, parser_data):
    print("Map with parallel extraction")
    print("Shuffle granularity (MB) {}".format(DEFAULT_GRANULE_SIZE_SHUFFLE))
    print("Reduce granularity (MB) {}".format(DEFAULT_GRANULE_SIZE_REDUCE))

    mz_precision = parser_data['mzPrecision']
    ints_precision = parser_data['intensityPrecision']
    coordinates = parser_data['coordinates']
    sp_id_to_idx = get_pixel_indices(coordinates)
    print("Got {} indexes".format(len(sp_id_to_idx)))

    min_mutipart_size = MIN_MULTIPART_SIZE
    default_granule_size_shuffle = DEFAULT_GRANULE_SIZE_SHUFFLE
    default_granule_size_reduce = DEFAULT_GRANULE_SIZE_REDUCE
    throughput_limit_per_function = PAR_LEVEL_LIMIT_PER_FUNCTION
    granule_size_to_bytes = GRANULE_SIZE_TO_BYTES

    # For each range, call mapper
    def _chunks_into_segments_imzml(range_i, ibm_cos):

        start_time = time.time()
        ranges = pickle.loads(
            ibm_cos.get_object(Bucket=dataset_bucket, Key="tmp/chunk_ranges/rng{}.pickle".format(range_i))[
                'Body'].read())

        offsets = ranges[0]
        lengths = ranges[1]
        chunk_num = ranges[2]
        lower_bound = ranges[3]
        upper_bound = ranges[4]
        print("started classification of chunk {} ({} to {} - {})".format(range_i,
                                                                          lower_bound,
                                                                          upper_bound,
                                                                          upper_bound - lower_bound))
        print("{} threads".format(DEFAULT_THREAD_NUM))
        # Read segments' file from COS
        segment_info = pickle.loads(ibm_cos.get_object(Bucket=segments_bucket, Key=partition_file)['Body'].read())
        print("Got segment info from COS *********************")

        # Create object reader
        cos_wrapper = CosFileBinaryWrapper(ibm_cos, dataset_bucket, dataset_file_path)
        print("Created wrapper *******************************")

        divided_segment_data = list()

        for coord_i in range(len(offsets)):
            divided_segment_data.append(list())

        print("Created base arrays ***************************")

        # In parallel inside function
        # For each coordinate inside chunks
        # segm_sem = threading.Semaphore()
        # print("Created Semaphore *****************************")

        print("Extracting coordinate data ***************")

        ed = array([])
        ed.shape = (0, 3)
        unvisited_indexes = list()
        if DEFAULT_THREAD_NUM != -1:
            lock = threading.Lock()

        initialization_time = time.time()

        def _classify_coordinate_data(i):
            # for i in range(len(offsets)):
            try:
                # print("Processing coordinate {} (really {})".format(i, lower_bound+i))
                coordinate_value = lower_bound + i
                # print("*****Coordinate number {} ({}/{})*****".format(coordinate_value, i, len(offsets)))
                # print("Accesing index {}".format(coordinate_value))
                sp_id = sp_id_to_idx[coordinate_value]

                # Get sp_idx, msz, ints structure
                cos_wrapper.seek(offsets[i][0])
                mzs = cos_wrapper.read(lengths[i][0])
                # print("mz_array read; length {}".format(lengths[i][0]))
                mz_array = frombuffer(mzs, dtype=mz_precision)
                # print("mz_array created")

                cos_wrapper.seek(offsets[i][1])
                ints = cos_wrapper.read(lengths[i][1])
                # print("ints array read; length {}")
                ints_array = frombuffer(ints, dtype=ints_precision)
                # print("ints_array created")

                # mzs_, ints_ = map(array, [mz_array, ints_array])
                sp_inds = ones_like(mz_array) * sp_id

                # Order the structure
                by_mz = argsort(mz_array)

                sp_mz_int = array([sp_inds,
                                   mz_array[by_mz],
                                   ints_array[by_mz]], mz_precision).T

                # print("ordered structure generated")
                return sp_mz_int

            # Exception handling
            except ValueError as e:
                # synchro
                lock.acquire()
                unvisited_indexes.append(i)
                lock.release()
                # synchro
                print("Value error")
                aux_array = array([])
                aux_array.shape = (0, 3)
                return aux_array

            except IndexError as e:
                # synchro
                lock.acquire()
                unvisited_indexes.append(i)
                lock.release()
                # synchro
                print("Index error")
                aux_array = array([])
                aux_array.shape = (0, 3)
                return aux_array

        def _classify_coordinate_range(rng):
            return list(map(_classify_coordinate_data, rng))

        if DEFAULT_THREAD_NUM != 1:
            # Generate ranges
            range_list = list()
            rest = len(offsets) % DEFAULT_THREAD_NUM
            for r in range(DEFAULT_THREAD_NUM):
                min_r = r * (len(offsets) // DEFAULT_THREAD_NUM)
                max_r = (r + 1) * (len(offsets) // DEFAULT_THREAD_NUM)
                if (max_r == DEFAULT_THREAD_NUM):
                    max_r = max_r + rest
                range_list.append(range(min_r, max_r))

            with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
                arrays = list(pool.map(_classify_coordinate_range, range_list))

            # Flatten list
            flat_list = [item for sublist in arrays for item in sublist]
            del arrays
        else:
            flat_list = list(map(_classify_coordinate_data, range(len(offsets))))

        data_collection_time = time.time()
        print("Data collected")
        ed = concatenate(flat_list)
        data_concatenation_time = time.time()
        print("Data concatenated")
        del flat_list
        print("Unnecessary data deleted")

        print("Recovering uncompleted indexes")
        # Fault tolerance
        if (len(unvisited_indexes) > 0):
            new_arrays = [ed]
            for x in unvisited_indexes:
                new_arrays.append(_classify_coordinate_data(x))
            ed = concatenate(new_arrays)

        print("Sorting fragment")
        total_rows_prev = ed.shape[0]
        # print("Ordering {} rows".format(ed.shape[0]))
        ed = ed[ed[:, 1].argsort(kind="mergesort")]
        bytes_per_row = ed[1, :].itemsize * ed[1, :].shape[0]
        data_sorted_time = time.time()

        print("Extracting segments")
        total_rows_after = 0
        segment_data = list()
        for segm_i in range(len(segment_info) - 1):
            # print("Coordinate {} segm {}".format(i, segm_i))
            left_bound = segment_info[segm_i]
            right_bound = segment_info[segm_i + 1]
            # print("left_bound {}, right_bound {}".format(left_bound, right_bound))
            segm_start, segm_end = searchsorted(ed[:, 1], (left_bound, right_bound))
            coord_segment = ed[segm_start:segm_end]
            total_rows_after += coord_segment.shape[0]
            segment_data.append(coord_segment)

        segment_extraction_time = time.time()

        # print("Uploading segments ({} total rows)".format(total_rows))

        def _order_upload_segm_info(segm_i):

            ibm_cos.put_object(Bucket=dataset_bucket,
                               Key="tmp/chunk{}/sgm{}.pickle".format(chunk_num, segm_i),
                               Body=pickle.dumps(segment_data[segm_i]))

        def _order_upload_segm_info_granuled(granule_info):
            segm_i = granule_info[0]
            lower_bound = granule_info[1]
            upper_bound = granule_info[2]
            granule_number = granule_info[3]
            ibm_cos.put_object(Bucket=dataset_bucket,
                               Key="tmp/chunk{}/sgm{}/gran{}.pickle".format(chunk_num, segm_i, granule_number),
                               Body=pickle.dumps(segment_data[segm_i][range(lower_bound, upper_bound), :]))

        if default_granule_size_shuffle == -1:
            with ThreadPoolExecutor(max_workers=min(len(segment_info), 128)) as pool:
                pool.map(_order_upload_segm_info, range(len(segment_info) - 1))
        # Granuled upload
        else:
            # Calculate lines per granule
            rows_per_granule = (default_granule_size_shuffle * granule_size_to_bytes) // bytes_per_row
            bounds = []
            # Generate bounds per segment
            for s in range(len(segment_data)):
                granule_number = 0
                lower_bound = 0
                upper_bound = rows_per_granule
                while upper_bound < segment_data[s].shape[0]:
                    bounds.append([s, lower_bound, upper_bound, granule_number])
                    lower_bound = upper_bound
                    upper_bound += rows_per_granule
                    granule_number += 1
                if upper_bound >= segment_data[s].shape[0]:
                    bounds.append([s, lower_bound, segment_data[s].shape[0], granule_number])
                    granule_number += 1

                print("Segment {}; {} parts".format(s, granule_number))
                ibm_cos.put_object(Bucket=dataset_bucket,
                                   Key="tmp/chunk{}/sgm{}/part_number.pickle".format(chunk_num, s),
                                   Body=pickle.dumps(granule_number))

            with ThreadPoolExecutor(max_workers=min(len(bounds), throughput_limit_per_function)) as pool:
                pool.map(_order_upload_segm_info_granuled, bounds)

            data_upload_time = time.time()

        return {'chunk_number': chunk_num,
                'before_rows': total_rows_prev,
                'after_rows': total_rows_after,
                'initialization_time': initialization_time - start_time,
                'data_collection_time': data_collection_time - start_time,
                'data_concatenation_time': data_concatenation_time - start_time,
                'data_sorted_time': data_sorted_time - start_time,
                'segment_extraction_time': segment_extraction_time - start_time,
                'data_upload_time': data_upload_time - start_time}

    print("Mapping {} functions".format(int(num_workers)))
    futures = pw.map(_chunks_into_segments_imzml, range(int(num_workers)))

    results = pw.get_result(futures)
    total_rows = 0
    effective_chunks = range(0, int(num_workers))
    finished_chunks = list()
    initialization_time = float(sum([x['initialization_time'] for x in results])) / float(num_workers)
    data_collection_time = float(sum([x['data_collection_time'] for x in results])) / float(num_workers)
    data_concatenation_time = float(sum([x['data_concatenation_time'] for x in results])) / float(num_workers)
    data_sorted_time = float(sum([x['data_sorted_time'] for x in results])) / float(num_workers)
    segment_extraction_time = float(sum([x['segment_extraction_time'] for x in results])) / float(num_workers)
    data_upload_time = float(sum([x['data_upload_time'] for x in results])) / float(num_workers)
    print("Average data initialization time {}".format(initialization_time))
    print("Average data collection time {}".format(data_collection_time))
    print("Average data concatenation time {}".format(data_concatenation_time))
    print("Average data sorted time {}".format(data_sorted_time))
    print("Average segment extraction time {}".format(segment_extraction_time))
    print("Average data upload time {}".format(data_upload_time))

    for res in results:
        total_rows += res['after_rows']
        finished_chunks.append(res['chunk_number'])
        # print("Chunk {} before {} after {}".format(res['chunk_number'],
        #                                            res['before_rows'],
        #                                            res['after_rows']))

    unfinished_chunks = list(set(effective_chunks) - set(finished_chunks))
    if (len(unfinished_chunks) > 0):
        print("Mapping remaining chunks {}".format(unfinished_chunks))
        futures = pw.map(_chunks_into_segments_imzml, unfinished_chunks)
        results = pw.get_result(futures)
        for res in results:
            total_rows += res['after_rows']
            finished_chunks.append(res['chunk_number'])
            print("Chunk {} before {} after {}".format(res['chunk_number'],
                                                       res['before_rows'],
                                                       res['after_rows']))

    print("Total rows {}".format(total_rows))

    print("All chunks mapped")