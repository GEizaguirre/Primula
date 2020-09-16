import time
from random import randrange, seed
from ibm_botocore.exceptions import ClientError
import pickle
import logging
import json

from pywren_ibm_cloud import ibm_cf_executor


def run_artificial_stragglers_test():


    config = json.load(open('config.json'))
    # Run this for DEBUG mode

    logging.basicConfig(level=logging.DEBUG)

    print("Starting executor")
    pywren_executor = ibm_cf_executor(config=config, runtime_memory=128)

    def my_map_function(ibm_cos, args):
        x = args['arguments']
        communication_path = args['metadata']['communication_path']
        end_path = args['metadata']['end_path']
        start_path = args['metadata']['start_path']
        bucket = args['metadata']['communication_bucket']
        index = args['metadata']['index']

        start_time = time.time()

        # Protocol to know if i am speculative.
        try:
            # I am speculative
            ibm_cos.get_object(Bucket=bucket,
                               Key="{}/{}.pickle".format(start_path, index))
            fname = "{}s".format(index)
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
            else:
                raise

        print(args)
        seed()
        sleep_seconds = 2
        sleep_iterations = 14
        straggler_iteration = 4

        for i in range(sleep_iterations):

            # check finish
            try:
                print("Looking for {}/{}.pickle".format(end_path, fname))
                ibm_cos.get_object(Bucket=bucket,
                                   Key="{}/{}.pickle".format(end_path, fname))
                ibm_cos.delete_object(Bucket=bucket,
                                      Key="{}/{}.pickle".format(end_path, fname))
                print("Function killed")
                return -(x + 7)

            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    print("sleeping phase {}/{}".format(i, sleep_iterations))
                    time.sleep(sleep_seconds)
                else:
                    raise

            # Create straggler
            if i == straggler_iteration:
                try:
                    ibm_cos.get_object(Bucket=bucket,
                                       Key="straggler_call")

                except ClientError as ex:
                    if ex.response['Error']['Code'] == 'NoSuchKey':
                        print("i am straggler ({})".format(x))
                        ibm_cos.put_object(Bucket=bucket,
                                           Key="straggler_call", Body=pickle.dumps(x))
                        ibm_cos.put_object(Bucket=bucket,
                                           Key="straggler_id_{}".format(x), Body=pickle.dumps(x))
                        # straggler will sleep more
                        sleep_seconds += 4
                    else:
                        raise

            # upload progress
            progress_info = {'progress': (i + 1) / sleep_iterations, 'time': time.time() - start_time}
            ibm_cos.put_object(Bucket=bucket,
                               Key="{}/{}.pickle".format(communication_path, index),
                               Body=pickle.dumps(progress_info))

        # if I am original task, remove indicator
        # if fname == str(index):
        #    ibm_cos.delete_object(Bucket=communication_bucket,
        #                           Key="{}/{}_start".format(communication_path,
        #                                                    index))

        progress_info = {'progress': 1, 'time': time.time() - start_time}
        ibm_cos.put_object(Bucket=bucket,
                           Key="{}/{}.pickle".format(communication_path, index),
                           Body=pickle.dumps(progress_info))

        print("summing 7 to {}".format(x))
        return x + 7


    ft = pywren_executor.map_monitor(my_map_function, list(range(20)), speculative=True)
    print(pywren_executor.get_result_monitor(ft))
    pywren_executor.clean()