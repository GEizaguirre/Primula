import os
import pickle

import math
import re
import time

import ibm_boto3
from ibm_botocore.exceptions import ClientError
from numpy import diff
from pandas import DataFrame
from pywren_ibm_cloud.sort.CosFileBinaryWrapper import CosFileBinaryWrapper
from pywren_ibm_cloud.sort.ImzMLParser import ImzMLParser
import numpy as np

# Granule size in MB
# Lambda's limit
# Minimum object size for uploading it in multipart in IBM COS.

# Maximum read number by each sampler.
from pywren_ibm_cloud.sort.config import MAX_RETRIES


def test_function(i):
    print("THIS IS THE FIRST VERSION OF THE TEST FUNCTION jeeeeii")
    return i


def print_dataset_info(imzml_parser):
    print("Calculating dataset info")
    mzs_count = 0
    coords = imzml_parser.coordinates
    for coord_i in range(len(coords)):
        print("counting coord {}".format(coord_i))
        mzs_, ints_ = imzml_parser.getspectrum(coord_i)
        mzs_count += len(mzs_)

    print("Total coordinates {}".format(len(coords)))
    print("Total values {}".format(mzs_count))


def clear_cos_prefix(info, ibm_cos):
    prefix = info['prefix']
    bucket = info['bucket']
    objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while 'Contents' in objs:
        keys = sorted([obj['Key'] for obj in objs['Contents']])
        formatted_keys = {'Objects': [{'Key': key} for key in keys]}
        print("Removing objects {}".format(keys))
        ibm_cos.delete_objects(Bucket=bucket, Delete=formatted_keys)

        objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=prefix)
    print("Temporal files removed")


def clear_cos_prefix_chunk(info, ibm_cos):
    prefix = info['prefix']
    bucket = info['bucket']
    chunk_i = info['chunk']
    complete_prefix = ''.join([prefix, "/chunk", str(chunk_i)])
    objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=complete_prefix)
    while 'Contents' in objs:
        keys = sorted([obj['Key'] for obj in objs['Contents']])
        formatted_keys = {'Objects': [{'Key': key} for key in keys]}
        print("Removing objects {}".format(keys))
        ibm_cos.delete_objects(Bucket=bucket, Delete=formatted_keys)

        objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=complete_prefix)
    print("Temporal chunk {} files removed".format(chunk_i))


# def _byte_range_gen_imzml(metadata_file_path,
#                           ibd_file_path,
#                           num_workers,
#                           bucket,
#                           ibm_cos):
def _gen_byte_ranges_imzml(info, ibm_cos):
    print("Generating byte ranges for sampling")
    # process = psutil.Process(os.getpid())
    # print("Used memory {} MB ".format(process.memory_info().rss / 1024 / 1024))
    # print("Total memory {} MB".format(psutil.virtual_memory().total / 1024 / 1024))

    metadata_file_path = info['metadata_file_path']
    ibd_file_path = info['ibd_file_path']
    num_workers = info['num_workers']
    bucket = info['bucket']

    imzml_metadata = ibm_cos.get_object(Bucket=bucket, Key=metadata_file_path)['Body'].read()
    f = open(os.path.basename(metadata_file_path), "wb")
    f.write(imzml_metadata)
    f.close()

    # # Load parser
    # wrapper for seek and read operations
    wrapped_buffer_ibd_object = CosFileBinaryWrapper(ibm_cos, bucket, ibd_file_path)
    #     #wrapped_buffer_imzml_object = CosFileBinaryWrapper(ibm_cos, bucket, ''.join([path, ".imzml"]))
    imzml_parser = ImzMLParser(os.path.basename(metadata_file_path), ibd_file=wrapped_buffer_ibd_object)

    # Define bounds
    coordinates = imzml_parser.coordinates
    segm_bounds_q = [i * 1 / num_workers for i in range(0, num_workers + 1)]
    segm_lower_bounds = [math.floor(len(coordinates) * segm_bounds_q[i]) for i in range(0, num_workers)]
    seqm_upper_bounds = [segm_lower_bounds[i] - 1 for i in range(1, num_workers)]
    seqm_upper_bounds.append(len(coordinates) - 1)

    # Get byte offsets and read lengths for each range
    mz_precision = imzml_parser.mzPrecision
    intensity_precision = imzml_parser.intensityPrecision
    range_values = list()
    global_ranges = list()
    for rng in range(0, num_workers):
        length_list = list()
        offset_list = list()
        for i in range(segm_lower_bounds[rng], seqm_upper_bounds[rng] + 1):
            offsets = [imzml_parser.mzOffsets[i], imzml_parser.intensityOffsets[i]]
            lengths = [imzml_parser.mzLengths[i] * imzml_parser.sizeDict[mz_precision],
                       imzml_parser.intensityLengths[i] * imzml_parser.sizeDict[intensity_precision]]

            offset_list.append(offsets)
            length_list.append(lengths)
            global_ranges.append([imzml_parser.mzOffsets[i],
                                  imzml_parser.intensityOffsets[i],
                                  imzml_parser.mzLengths[i] * imzml_parser.sizeDict[mz_precision],
                                  imzml_parser.intensityLengths[i] * imzml_parser.sizeDict[intensity_precision]])
        range_values.append([offset_list, length_list, rng])

    for num_rng in range(len(range_values)):
        ibm_cos.put_object(Bucket=bucket,
                           Key="tmp/sampling_ranges/sample_range{}.pickle".format(num_rng),
                           Body=pickle.dumps(range_values[num_rng]))
    ibm_cos.put_object(Bucket=bucket,
                       Key="tmp/segment_ranges/global_ranges.pickle",
                       Body=pickle.dumps(global_ranges))

    return {'sp_n': len(imzml_parser.coordinates),
            'mzPrecision': imzml_parser.mzPrecision,
            'intensityPrecision': imzml_parser.intensityPrecision,
            'coordinates': imzml_parser.coordinates}


BOUND_EXTRACTION_MARGIN = 100


# Adjust csv read bounds not to cut lines in the middle.
def _extract_real_bounds_csv(ibm_cos, lower_bound, upper_bound, total_size, bucket, key):
    lb = lower_bound
    if lb != 0:

        # lower_bound definition
        plb = lower_bound - BOUND_EXTRACTION_MARGIN
        pub = lower_bound + BOUND_EXTRACTION_MARGIN
        if plb < 0:
            plb = 0
        if pub > total_size:
            pub = total_size
        marked_pos = lower_bound - plb

        ### cos read of bytes=plb,pub
        chunk = ibm_cos.get_object(Bucket=bucket,
                                   Key=key,
                                   Range=''.join(
                                       ['bytes=', str(plb), '-',
                                        str(pub)]))['Body'].read()

        while (plb != 0) and (b'\n' not in chunk[:marked_pos]):
            plb = plb - BOUND_EXTRACTION_MARGIN
            # cos read of bytes=plb,pub
            chunk = ibm_cos.get_object(Bucket=bucket,
                                       Key=key,
                                       Range=''.join(
                                           ['bytes=', str(plb), '-',
                                            str(pub)]))['Body'].read()
            marked_pos = lower_bound - plb

        if plb != 0:
            for i in reversed(range(0, marked_pos)):
                if chunk[i:i + 1] == b'\n':
                    plb = plb + i + 1
                    break
        lb = plb

    ub = upper_bound
    if ub < total_size:

        # upper bound definition
        plb = upper_bound - BOUND_EXTRACTION_MARGIN
        pub = upper_bound + BOUND_EXTRACTION_MARGIN
        if plb < 0:
            plb = 0
        if pub > total_size:
            pub = total_size

        marked_pos = upper_bound - plb

        if pub != total_size:
            chunk = ibm_cos.get_object(Bucket=bucket,
                                       Key=key,
                                       Range=''.join(
                                           ['bytes=', str(plb), '-',
                                            str(pub)]))['Body'].read()

            ub = plb
            i = 0
            while (plb != 0) and (b'\n' not in chunk[:marked_pos]) and i < 5:
                plb = plb - BOUND_EXTRACTION_MARGIN
                # cos read of bytes=plb,pub
                chunk = ibm_cos.get_object(Bucket=bucket,
                                           Key=key,
                                           Range=''.join(
                                               ['bytes=', str(plb), '-',
                                                str(pub)]))['Body'].read()
                marked_pos = upper_bound - plb
                i = i + 1
            for i in reversed(range(0, marked_pos)):
                if chunk[i:i + 1] == b'\n':
                    plb = plb + i + 1
                    break

            ub = plb - 1
        else:
            ub = pub

    return lb, ub


def remove_prefix_cos(ibm_cos, bucket, prefix):
    for pfx in prefix:
        objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=pfx)
        while 'Contents' in objs:
            keys = [obj['Key'] for obj in objs['Contents']]
            formatted_keys = {'Objects': [{'Key': key} for key in keys]}
            ibm_cos.delete_objects(Bucket=bucket, Delete=formatted_keys)

            objs = ibm_cos.list_objects_v2(Bucket=bucket, Prefix=pfx)


def generate_fill_values(types, nms):
    fill_values = {}
    for nm in nms:
        if re.match("int[0-9]*", types[nm]) is not None or \
                re.match("uint[0-9]*", types[nm]) is not None:
            fill_values[nm] = np.iinfo(types[nm]).max
        if re.match("float[0-9]*", types[nm]) is not None:
            fill_values[nm] = np.finfo(types[nm]).max
        if re.match("\|S[0-9]*", types[nm]) is not None:
            total_length = int(types[nm][2:])
            fill_values[nm] = bytearray([126 for i in range(total_length)]).decode('utf-8')
    return fill_values


def _create_dataframe(df_columns, keys, types, key_nm):
    # Generated a dataframe starting from
    # the heaviest column.

    print("Creating dataframe")
    # Get heaviest column.
    nms = list(types.keys())
    first_nm = str(sort_names_by_type_size(types, nms)[0])

    print("nms: {}".format(nms))
    print("fist_nm: {}".format(first_nm))
    print("types: {}".format(types))
    print("key nm: {}".format(key_nm))

    if key_nm == first_nm:
        print("starting from keys")
        df = DataFrame(keys)
        keys = np.empty(0)
    else:
        print("starting form column {}".format(first_nm))
        df = DataFrame(df_columns[first_nm])
        df_columns[first_nm] = np.empty(0)

    df.columns = [first_nm]
    print("Created dataframe")

    for index, nm in enumerate(nms):
        if nm != first_nm:
            print("adding column {}".format(nm))
            if nm == key_nm:
                print("is key")
                df.insert(index, nm, keys)
                keys = np.empty(0)
            else:
                print("is column")
                df.insert(index, nm, df_columns[nm])
                df_columns[nm] = np.empty(0)

    print("Finished dataframe")
    print(df.dtypes)
    return df


def sort_names_by_type_size(types, nms):
    type_sizes = {}
    for nm in nms:
        type_sizes[nm] = int(np.dtype(types[nm]).itemsize)
    sorted_types = {k: v for k, v in sorted(type_sizes.items(), key=lambda item: item[1])}
    return list(reversed(list(sorted_types.keys())))


def get_normalized_type_names(types):
    type_names = list(types.values())
    return [np.dtype(n).name for n in type_names]


def correct_types(df, normalized_type_names):
    df_types = [n.name for n in list(df.dtypes)]
    for index, nm in enumerate(normalized_type_names):
        if nm is not df_types[index]:
            return False
    return True


def lsos(n=10):
    import pandas as pd
    import sys

    all_obj = globals()

    object_name = list(all_obj).copy()
    object_size = [sys.getsizeof(all_obj[x]) for x in object_name]

    d = pd.DataFrame(dict(name=object_name, size=object_size))
    d.sort_values(['size'], ascending=[0], inplace=True)

    return (d.head(n))


def permutate(A, P):  # Iterate through every element in the given arrays
    for i in range(len(A)):
        # We look at P to see what is the new index
        index_to_swap = P[i]
        # Check index if it has already been swapped before
        while index_to_swap < i:
            index_to_swap = P[index_to_swap]
            # Swap the position of elements
            A[i], A[index_to_swap] = A[index_to_swap], A[i]


is_sorted_np = lambda x: (diff(x) >= 0).all()


def _multipart_upload(ibm_cos, output_path, output_bucket, df, part_number, segm_i):
    print("Multipart upload of segment {}".format(segm_i))

    # Generate segment byte object
    granule_size = int(df.shape[0] / part_number)

    granule_number = 0
    lower_bound = 0
    upper_bound = granule_size
    shp = df.shape[0]
    bounds = []
    while upper_bound < shp:
        bounds.append([lower_bound, upper_bound])
        lower_bound = upper_bound
        upper_bound += granule_size
        granule_number += 1
    if upper_bound >= shp:
        bounds.append([lower_bound, shp])
        granule_number += 1

    upload_id = ibm_cos.create_multipart_upload(Bucket=output_bucket,
                                                Key="{}/{}.pickle".format(output_path,
                                                                          segm_i))['UploadId']
    print("{} parts".format(granule_number))

    def _upload_segment_part(part_i):
        etag = -1
        print("Uploading {}".format(part_i))
        try:
            etag = ibm_cos.upload_part(Bucket=output_bucket,
                                       Key="{}/{}.pickle".format(output_path, segm_i),
                                       Body=pickle.dumps(df[bounds[part_i][0]:bounds[part_i][1]]),
                                       UploadId=upload_id,
                                       PartNumber=(part_i + 1))['ETag']
            print("Uploaded part {}".format(part_i))
        except ClientError:
            # Too many requests error
            available_retries = MAX_RETRIES
            upload_done = False
            while not upload_done:
                if available_retries <= 0:
                    break
                else:
                    time.sleep(0.05)
                    try:
                        print("Retrying part {}".format(part_i))
                        etag = ibm_cos.upload_part(Bucket=output_bucket,
                                                   Key="{}/{}.pickle".format(output_path, segm_i),
                                                   Body=pickle.dumps(df[bounds[part_i][0]:bounds[part_i][1]]),
                                                   UploadId=upload_id,
                                                   PartNumber=(part_i + 1))['ETag']
                        upload_done = True
                    except ClientError:
                        available_retries -= 1
        return etag

    etags = list(map(_upload_segment_part, range(len(bounds))))

    if -1 in etags:
        print("Could not upload segment {}".format(segm_i))
        ibm_cos.abort_multipart_upload(Bucket=output_bucket,
                                       Key="{}/{}.pickle".format(output_path, segm_i),
                                       UploadId=upload_id)
    else:
        multipart_json = {'Parts': []}
        for t in range(len(etags)):
            multipart_json['Parts'].append({'ETag': etags[t], 'PartNumber': t + 1})
        ibm_cos.complete_multipart_upload(Bucket=output_bucket,
                                          Key="{}/{}.pickle".format(output_path, segm_i),
                                          UploadId=upload_id,
                                          MultipartUpload=multipart_json)

        print("Multipart upload of segment {} completed".format(segm_i))


def get_dataset_size(pw, bucket, csv_file_path):
    ibm_cos = ibm_boto3.client(service_name='s3',
                               aws_access_key_id=pw.config['ibm_cos']['access_key'],
                               aws_secret_access_key=pw.config['ibm_cos']['secret_key'],
                               endpoint_url=pw.config['ibm_cos']['endpoint'])
    return ibm_cos.get_object(Bucket=bucket, Key=csv_file_path)['ContentLength']