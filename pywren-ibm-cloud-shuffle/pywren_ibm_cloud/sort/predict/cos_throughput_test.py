import os
import pickle
from concurrent.futures.thread import ThreadPoolExecutor
import time
import ibm_boto3 as boto3
import hashlib
import uuid
from .data_generator import RandomDataGenerator
import pywren_ibm_cloud as pywren

def write_throughput(config, bucket_name, mb_per_unit, number_of_functions, number_of_ops, key_prefix,
                     outfile, key_file):
    print("bucket_name = {}".format(bucket_name))
    print("kb_per_unit = {}".format(mb_per_unit))
    print("number_of_functions = {}".format(number_of_functions))
    print("number_of_ops = {}".format(number_of_ops))
    print("key_prefix = {}".format(key_prefix))

    def run_command(key_name):
        print("Run command started (parallel write throughput)")
        bytes_n = mb_per_unit * 1024 * 1024
        print("Bytes per op: {}".format(bytes_n))

        print("Creating data random generator")
        d = [ RandomDataGenerator(bytes_n) for _ in range(number_of_ops)  ]

        print("Creating client")
        client = boto3.client(service_name='s3',
                            aws_access_key_id=config['ibm_cos']['access_key'],
                            aws_secret_access_key=config['ibm_cos']['secret_key'],
                            endpoint_url=config['ibm_cos']['endpoint'])
        t1 = time.time()

        def _put_operation(i):
            my_key = "{}{}".format(key_name, str(i))
            client.put_object(Bucket=bucket_name,
                              Key=my_key,
                              Body=d[i])

        # pool = Pool()
        with ThreadPoolExecutor(max_workers=(number_of_ops)) as pool:
            myfuturelist = [pool.submit(_put_operation, x) for x in range(number_of_ops)]
        [ f.result() for f in myfuturelist ]
        # pool.close()
        # pool.join()

        t2 = time.time()
        print("All put operations performed")

        op_rate = number_of_ops / (t2 - t1)
        return t1, t2, op_rate

    pw = pywren.ibm_cf_executor(config=config, runtime_memory=2048)
    # create list of random keys
    keynames = [key_prefix + str(uuid.uuid4()) for _ in range(number_of_functions)]

    if key_file is not None:
        fid = open(key_file, 'w')
        for k in keynames:
            fid.write("{}\n".format(k))

    fut = pw.map(run_command, keynames)

    res = pw.get_result(fut)

    f = open(outfile, 'wb', buffering=0)
    pickle.dump(res, f)
    f.flush()
    f.close()


def read_throughput(config, bucket_name, mb_per_unit, number_of_functions, number_of_ops, key_prefix,
                    outfile, key_file):

    #print("updated blocksize")
    #blocksize = number_of_m_per_block * 1024 ** 2
    blocksize = mb_per_unit *1024*1024

    def run_command(key):
        print("Run command started (parallel read throughput)")
        client = boto3.client(service_name='s3',
                              aws_access_key_id=config['ibm_cos']['access_key'],
                              aws_secret_access_key=config['ibm_cos']['secret_key'],
                              endpoint_url=config['ibm_cos']['endpoint'])

        print("Client created")

        m = hashlib.md5()
        bytes_read = 0

        t1 = time.time()
        print("Starting reads")

        def _read_operation(i):
            obj = client.get_object(Bucket=bucket_name, Key="{}{}".format(key, str(i)))

            fileobj = obj['Body']

            buf = fileobj.read(blocksize)
            # while len(buf) > 0:
            #     #bytes_read += len(buf)
            #     m.update(buf)
            #     buf = fileobj.read(blocksize)

            client.delete_object(Bucket=bucket_name, Key=key)

        with ThreadPoolExecutor(max_workers=(number_of_ops)) as pool:
            myfuturelist = [pool.submit(_read_operation, x) for x in range(number_of_ops)]
        [f.result() for f in myfuturelist]
        # pool = Pool()
        # pool.map(_read_operation, range(number_of_ops))
        # pool.close()
        # pool.join()

        t2 = time.time()

        op_rate = number_of_ops / (t2 - t1)

        print("Reads finished and rate calculated")
        return t1, t2, op_rate,

    pw = pywren.ibm_cf_executor(config=config, runtime_memory=2048)

    fid = open(key_file, 'r')
    keylist_raw = [k.strip() for k in fid.readlines()]
    keylist = [keylist_raw[i % len(keylist_raw)] for i in range(number_of_functions)]

    fut = pw.map(run_command, keylist)
    print("All functions mapped")
    res = pw.get_result(fut)

    f = open(outfile, 'wb', buffering=0)
    pickle.dump(res, f)
    f.flush()
    f.close()