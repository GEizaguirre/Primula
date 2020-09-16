import math

from pywren_ibm_cloud.sort.CosFileBinaryWrapper import CosFileBinaryWrapper
from pywren_ibm_cloud.sort.ImzMLParser import ImzMLParser
from pywren_ibm_cloud.sort.tests.phase1_alternatives.phase1_imzml import get_pixel_indices
import pickle
import numpy as np
import pandas as pd

MEMORY_PER_FUNCTION = 2048
MEMORY_ESCALATION=0.5
METADATA_FILE_NAME = "metadata.imzml"
MAX_COORD_PER_WORKER = 2000


def prepare_conversion_imzml_csv(input_bucket, input_key, output_bucket, output_key, ibm_cos):
    imzml_metadata_file_path = ''.join([input_key, '.imzml'])
    imzml_ibd_file_path = ''.join([input_key, '.ibd'])

    imzml_metadata = ibm_cos.get_object(Bucket=output_bucket, Key=imzml_metadata_file_path)['Body'].read()
    f = open(METADATA_FILE_NAME, "wb")
    f.write(imzml_metadata)
    f.close()

    object_size = ibm_cos.get_object(Bucket=output_bucket, Key=imzml_ibd_file_path)['ContentLength']

    # Define parser
    wrapped_buffer_ibd_object = CosFileBinaryWrapper(ibm_cos, input_bucket, imzml_ibd_file_path)
    imzml_parser = ImzMLParser(METADATA_FILE_NAME,
                               ibd_file=wrapped_buffer_ibd_object)

    coordinates = imzml_parser.coordinates
    len_coord = len(coordinates)
    mz_precision = imzml_parser.mzPrecision
    intensity_precision = imzml_parser.intensityPrecision
    mz_size = imzml_parser.sizeDict[mz_precision]
    intensity_size = imzml_parser.sizeDict[intensity_precision]

    number_conversors_mem = object_size // (MEMORY_PER_FUNCTION * MEMORY_ESCALATION * (1024 ** 2))
    number_conversors_mem /= 5
    number_conversors_mem = int(number_conversors_mem)
    if (object_size % (MEMORY_PER_FUNCTION * MEMORY_ESCALATION * (1024 ** 2))) > 0:
        number_conversors_mem += 1
    number_conversors_coord = len_coord // MAX_COORD_PER_WORKER
    number_conversors_coord /= 5
    number_conversors_coord = int(number_conversors_coord)
    if (len_coord % MAX_COORD_PER_WORKER) > 0:
        number_conversors_coord += 1
    number_conversors = max(number_conversors_coord, number_conversors_mem)

    # get line example for size calculations
    if len_coord == 0:
        raise ValueError("File has no coordinates")

    wrapped_buffer_ibd_object.seek(imzml_parser.mzOffsets[0])
    mz_array = np.frombuffer(wrapped_buffer_ibd_object.read(mz_size), dtype=mz_precision)
    wrapped_buffer_ibd_object.seek(imzml_parser.intensityOffsets[0])
    ints_array = np.frombuffer(wrapped_buffer_ibd_object.read(intensity_size), dtype=intensity_precision)
    sp_inds = np.ones_like(mz_array) * get_pixel_indices(coordinates)[0]
    sp_mz_int = np.array([sp_inds,
                          mz_array,
                          ints_array], mz_precision).T
    np.savetxt('line_sampler.csv', sp_mz_int, delimiter=',')
    with open('line_sampler.csv', 'r') as f:
        line_size = len(f.readline())

    # extract and upload coordinate values
    segm_bounds_q = [i * 1 / number_conversors for i in range(0, number_conversors + 1)]
    segm_lower_bounds = [math.floor(len(coordinates) * segm_bounds_q[i]) for i in range(0, number_conversors)]
    segm_upper_bounds = [segm_lower_bounds[i] - 1 for i in range(1, number_conversors)]
    segm_upper_bounds.append(len(coordinates) - 1)
    print(segm_lower_bounds)
    print(segm_upper_bounds)

    # Get byte offsets and read lengths for each range
    range_values = list()
    total_rows = 0
    for rng in range(0, number_conversors):
        length_list = list()
        offset_list = list()
        for i in range(segm_lower_bounds[rng], segm_upper_bounds[rng] + 1):
            total_rows += imzml_parser.mzLengths[i]
            offsets = [imzml_parser.mzOffsets[i], imzml_parser.intensityOffsets[i]]
            lengths = [imzml_parser.mzLengths[i] * imzml_parser.sizeDict[mz_precision],
                       imzml_parser.intensityLengths[i] * imzml_parser.sizeDict[intensity_precision]]

            offset_list.append(offsets)
            length_list.append(lengths)

        range_values.append([offset_list, length_list, rng])

    # Upload info file
    info_dict = { 'num_coordinates': len_coord,
                 'num_rows': total_rows,
                 'mz_precision': mz_precision,
                 'intensity_precision': intensity_precision,
                 'mz_size': mz_size,
                 'intensity_size': intensity_size,
                 'line_size': line_size }

    ibm_cos.put_object(Bucket=output_bucket,
                       Key=''.join([output_key, '.info']),
                       Body=pickle.dumps(info_dict))

    for num_rng in range(len(range_values)):
        ibm_cos.put_object(Bucket=output_bucket,
                           Key="tmp/conversion_range{}.pickle".format(num_rng),
                           Body=pickle.dumps(range_values[num_rng]))

    # start multipart upload
    upload_info = ibm_cos.create_multipart_upload(Bucket=output_bucket,
                                                   Key=''.join([output_key, '.csv']))
    upload_info['idx'] = get_pixel_indices(coordinates)
    upload_info['number_conversors'] = number_conversors
    upload_info['lower_bounds'] = segm_lower_bounds
    upload_info['upper_bounds'] = segm_upper_bounds
    upload_info['ibd_file'] = imzml_ibd_file_path
    upload_info['mz_precision'] = mz_precision
    upload_info['intensity_precision'] = intensity_precision
    ibm_cos.put_object(Bucket=output_bucket,
                       Key="tmp/upload_info.pickle",
                       Body=pickle.dumps(upload_info))

    return {'number_conversors': number_conversors,
            'UploadId': upload_info['UploadId'],
            'Key': upload_info['Key'],
            'Bucket': upload_info['Bucket'],
            'total_rows': total_rows}


def convert_chunk_imzml_csv(chunk_i, bucket, ibm_cos):
    range_values = pickle.loads(
        ibm_cos.get_object(Bucket=bucket, Key="tmp/conversion_range{}.pickle".format(chunk_i))['Body'].read())

    upload_info = pickle.loads(
        ibm_cos.get_object(Bucket=bucket, Key="tmp/upload_info.pickle")['Body'].read())

    cos_wrapper = CosFileBinaryWrapper(ibm_cos, bucket, upload_info['ibd_file'])
    sp_id_to_idx = upload_info['idx']
    lower_bound = upload_info['lower_bounds'][chunk_i]

    offsets = range_values[0]
    lengths = range_values[1]

    print("Conversor {}, {} coordinates".format(chunk_i, len(offsets)))

    def _classify_coordinate_data(i):

        #print("Processing coordinate {} (really {})".format(i, lower_bound+i))
        coordinate_value = lower_bound + i
        # print("*****Coordinate number {} ({}/{})*****".format(coordinate_value, i, len(offsets)))
        # print("Accesing index {}".format(coordinate_value))
        sp_id = sp_id_to_idx[coordinate_value]

        # Get sp_idx, msz, ints structure
        cos_wrapper.seek(offsets[i][0])
        mzs = cos_wrapper.read(lengths[i][0])
        # print("mz_array read; length {}".format(lengths[i][0]))
        mz_array = np.frombuffer(mzs, dtype=upload_info['mz_precision'])
        # print("mz_array created")

        cos_wrapper.seek(offsets[i][1])
        ints = cos_wrapper.read(lengths[i][1])
        # print("ints array read; length {}")
        ints_array = np.frombuffer(ints, dtype=upload_info['intensity_precision'])
        # print("ints_array created")

        # mzs_, ints_ = map(np.array, [mz_array, ints_array])
        sp_inds = np.ones_like(mz_array).astype('int32') * sp_id

        # sp_mz_int = pd.array([sp_inds,
        #                       mz_array,
        #                       ints_array], upload_info['mz_precision']).T

        sp_mz_int = pd.DataFrame({'idx': sp_inds, 'mzs': mz_array, 'ints': ints_array})

        # print("ordered structure generated")
        return sp_mz_int

    flat_list = list(map(_classify_coordinate_data, range(len(offsets))))
    df = pd.concat(flat_list)
    df.to_csv('data.csv', index=False, header=False)
    print("Uploading part")
    with open('data.csv', 'rb') as f:
        ETag = ibm_cos.upload_part(Body=f,
                                   Bucket=upload_info['Bucket'],
                                   Key=upload_info['Key'],
                                   PartNumber=chunk_i+1,
                                   UploadId=upload_info['UploadId'])['ETag']
    with open('data.csv') as f:
        f.seek(0 , 2)
        size_bytes = f.tell()

    return {'ETag':ETag, 'size':df.shape[0], 'size_bytes':size_bytes }


def convert_imzml_csv(pw, input_bucket, input_key, output_bucket, output_key, ibm_cos):

    print("Converting {} to csv".format(input_key))
    print("{} coord per worker".format(MAX_COORD_PER_WORKER))
    params = {
        'input_bucket': input_bucket,
        'input_key': input_key,
        'output_bucket': output_bucket,
        'output_key': output_key
    }

    futures = pw.call_async(prepare_conversion_imzml_csv, params)
    r = pw.get_result(futures)
    print("Total rows {}".format(r['total_rows']))
    print("Conversion prepared, callling conversors")
    num_workers = r['number_conversors']
    print("{} conversors".format(num_workers))
    upload_id = r['UploadId']
    futures = pw.map(convert_chunk_imzml_csv, range(int(num_workers)), extra_params= [output_bucket])
    etags = pw.get_result(futures)
    multipart_json = {'Parts': []}
    # print("Finishing conversion")
    size_sum = 0
    for t in range(len(etags)):
         multipart_json['Parts'].append({'ETag': etags[t]['ETag'], 'PartNumber': t + 1})
         print("converter {} coordinates {} size {} MB".format(t, etags[t]['size'], etags[t]['size_bytes'] / (1024**2)))
         size_sum += etags[t]['size']
    print("Total {} coordinates".format(size_sum))

    print(multipart_json)
    print(upload_id)
    print(r['Bucket'])
    print(r['Key'])

    ibm_cos.complete_multipart_upload(Bucket=r['Bucket'],
                                      Key=r['Key'],
                                      UploadId=upload_id,
                                      MultipartUpload=multipart_json)
    pw.clean()
    print("Multipart upload completed")
