import pickle

from ibm_botocore.exceptions import ClientError


def _check_if_speculative(ibm_cos, args, index):

    start_path = args['metadata']['start_path']
    bucket = args['metadata']['communication_bucket']

    try:
        # Am I speculative?
        ibm_cos.get_object(Bucket=bucket,
                           Key="{}/{}.pickle".format(start_path, index))
        fname = "{}s".format(index)
        is_speculative = True
        ibm_cos.delete_object(Bucket=bucket,
                              Key="{}/{}.pickle".format(start_path, index))

    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            # I am not speculative
            ibm_cos.put_object(Bucket=bucket,
                               Key="{}/{}.pickle".format(start_path,
                                                         index),
                               Body=pickle.dumps(index))
            fname = str(index)
            is_speculative = False
        else:
            raise

    return fname, is_speculative


def _check_end_signal(ibm_cos, args, fname):

    end_path = args['metadata']['end_path']
    bucket = args['metadata']['communication_bucket']

    # check finish
    try:
        print("Looking for {}/{}.pickle".format(end_path, fname))
        ibm_cos.get_object(Bucket=bucket,
                           Key="{}/{}.pickle".format(end_path, fname))
        ibm_cos.delete_object(Bucket=bucket,
                              Key="{}/{}.pickle".format(end_path, fname))
        print("Function killed")
        return True

    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return False
        else:
            raise

def _upload_progress(ibm_cos, index, progress_score, runtime, args):

    communication_path = args['metadata']['communication_path']
    bucket = args['metadata']['communication_bucket']

    # upload progress
    progress_info = {'progress': progress_score, 'time': runtime}
    ibm_cos.put_object(Bucket=bucket,
                       Key="{}/{}.pickle".format(communication_path, index),
                       Body=pickle.dumps(progress_info))