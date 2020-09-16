from pywren_ibm_cloud.sort.tests.teragen.config import METADATA_FILE_BUCKET, METADATA_FILE_KEY
import pickle

from pywren_ibm_cloud.sort.tests.teragen.random_generator import RandomGenerator


def map_split(ibm_cos, x):
    metadata_info = pickle.loads(ibm_cos.get_object(Bucket=METADATA_FILE_BUCKET, Key=METADATA_FILE_KEY)['Body'].read())
    print(metadata_info)
    output_bucket = metadata_info['output_bucket']
    output_key = metadata_info['output_key']
    upload_id = metadata_info['upload_id']
    first_row = metadata_info['splits'][str(x)][0]
    num_rows = metadata_info['splits'][str(x)][1]
    # preallocation of structure
    filler = [65 + i for i in range(10) for j in range(26)]
    row = [0 for i in range(100)]
    rows = bytearray()
    rand = RandomGenerator(first_row)
    for index, row_id in enumerate(range(first_row, first_row + num_rows)):

        for i in range(3):
            temp = rand.next() / 52
            row[3 + 4 * i] = 33 + (temp % 94)
            temp = temp / 94
            row[2 + 4 * i] = 33 + (temp % 94)
            temp = temp / 94
            row[1 + 4 * i] = 33 + (temp % 94)
            temp = temp / 94
            row[4 * i] = 33 + (temp % 94)
        if int(row[0]) is 34:
            row[0] = int(35)
        if int(row[9]) is 34:
            row[9] = int(35)
        # add tabulator
        row[10] = 9
        # row id
        row_digits = [int(d) for d in str(row_id)]
        base_index_n = 11
        num_spaces = 10 - len(row_digits)
        for i in range(num_spaces):
            row[base_index_n + i] = 48
        for index, digit in enumerate(row_digits):
            row[base_index_n + index + num_spaces] = digit + 48
        # filler blocks
        base = int(row_id * 8 % 16)
        for i in range(7):
            current_disp = base_index_n + 10 + 10 * i
            fill_val = filler[(base + i) % 26]
            for i in range(10):
                row[current_disp + i] = fill_val
        current_disp = base_index_n + 10 + 70
        fill_val = filler[(base + 7) % 26]
        for i in range(8):
            row[current_disp + i] = fill_val
        # newline
        row[99] = 10
        rows += (bytearray([int(v) for v in row]))

    byte_rows = bytes(rows)

    print("generated {} rows, total size {} B".format(len(rows)/100, len(byte_rows)))

    print("Uploading {} to bucket {}, part {}".format(output_key, output_bucket, x))
    upload_part = ibm_cos.upload_part(Bucket=output_bucket,
                                      Key=output_key,
                                      Body=byte_rows,
                                      UploadId=upload_id,
                                      PartNumber=int(x)+1)
    return upload_part['ETag']
    # return x


def complete_upload(results, ibm_cos):
    metadata_info = pickle.loads(ibm_cos.get_object(Bucket=METADATA_FILE_BUCKET, Key=METADATA_FILE_KEY)['Body'].read())
    print(metadata_info)
    output_bucket = metadata_info['output_bucket']
    output_key = metadata_info['output_key']
    upload_id = metadata_info['upload_id']

    parts_info = dict()
    parts_info['Parts'] = [
        {
            'ETag': results[index],
            'PartNumber': index+1
        }
        for index in range(len(results))
    ]

    ibm_cos.complete_multipart_upload(Bucket=output_bucket,
                                      Key=output_key,
                                      UploadId=upload_id,
                                      MultipartUpload=parts_info)
