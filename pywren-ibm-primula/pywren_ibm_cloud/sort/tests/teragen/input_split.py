import pickle

from pywren_ibm_cloud.sort.tests.teragen.config import METADATA_FILE_BUCKET, METADATA_FILE_KEY


def input_split(ibm_cos, args):
    total_rows = args['num_rows']
    num_splits = args['num_splits']
    output_bucket = args['output_bucket']
    output_key = args['output_key']
    rows_per_split = int(total_rows/num_splits)
    print("Generating {} using {} maps with step of {}"
          .format(total_rows, num_splits, rows_per_split))
    splits = {}
    current_row = args['start_row']
    row_counter=0
    for split in range(0, num_splits - 1):
        splits[str(split)] = [ int(current_row), int(rows_per_split)]
        current_row += rows_per_split
        row_counter += rows_per_split
    splits[str(num_splits-1)] = [int(current_row), int(total_rows-row_counter)]

    upload_id = ibm_cos.create_multipart_upload(Bucket=output_bucket,
                                                Key=output_key)['UploadId']

    metadata_info=dict()
    metadata_info['output_key'] = output_key
    metadata_info['output_bucket'] = output_bucket
    metadata_info['splits'] = splits
    metadata_info['upload_id'] = upload_id
    ibm_cos.put_object(Bucket=METADATA_FILE_BUCKET,
                       Key=METADATA_FILE_KEY,
                       Body=pickle.dumps(metadata_info))
