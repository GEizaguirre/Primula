
import pickle
import time
import sys
import hashlib
import uuid
from .data_generator import RandomDataGenerator
import pywren_ibm_cloud as pywren

def write(config, bucket_name, mb_per_file, number, key_prefix,
          outfile, key_file):
    print("bucket_name = {}".format(bucket_name))
    print("mb_per_file = {}".format(mb_per_file))
    print("number = {}".format(number))
    print("key_prefix = {}".format(key_prefix))

    def run_command(ibm_cos, key_name):
        print("Run command started")
        bytes_n = mb_per_file * 1024 ** 2

        print("Creating data random generator")
        d = RandomDataGenerator(bytes_n)

        t1 = time.time()

        print("Putting object")
        ibm_cos.put_object(Bucket=bucket_name,
                          Key=key_name,
                          Body=d)
        t2 = time.time()

        print("Getting rate")
        mb_rate = bytes_n / (t2 - t1) / 1e6
        return t1, t2, mb_rate

    pw = pywren.ibm_cf_executor(config=config, runtime_memory=2048)

    # create list of random keys
    keynames = [key_prefix + str(uuid.uuid4()) for _ in range(number)]

    if key_file is not None:
        fid = open(key_file, 'w')
        for k in keynames:
            fid.write("{}\n".format(k))

    fut = pw.map(run_command, keynames)

    res = pw.get_result(fut)

    print("No buffering file write")
    f = open(outfile, 'wb', buffering=0)
    pickle.dump(res, f)
    f.flush()
    f.close()

def read(config, bucket_name, number,
         outfile, key_file, cos_key, read_times, number_of_m_per_block):
    if key_file is None and cos_key is None:
        print("must specify either a single key to repeatedly read ( --cos_key) or a text file with keynames (--key_file)")
        sys.exit(1)

    blocksize = number_of_m_per_block * 1024 *1024

    def run_command(ibm_cos, key):

        print("Client created")
        print("Read granularity {} MB".format(number_of_m_per_block))

        m = hashlib.md5()
        bytes_read = 0

        t1 = time.time()
        print("Starting reads")
        for i in range(read_times):
            obj = ibm_cos.get_object(Bucket=bucket_name, Key=key)


            fileobj = obj['Body']

            buf = fileobj.read(blocksize)
            bytes_read += len(buf)
            m.update(buf)

            ibm_cos.get_object(Bucket=bucket_name, Key=key)
            # while len(buf) > 0:
            #     buf = fileobj.read(blocksize)
            #     bytes_read += len(buf)
            #     m.update(buf)

        print("Blocksize {} and bytes read {}".format(blocksize, bytes_read))
        t2 = time.time()

        a = m.hexdigest()
        mb_rate = bytes_read / (t2 - t1) / 1e6

        print("Reads finished and rate calculated")
        return t1, t2, mb_rate,

    pw = pywren.ibm_cf_executor(config=config, runtime_memory=2048)

    if cos_key is not None:
        keylist = [cos_key] * number
    else:
        fid = open(key_file, 'r')
        keylist_raw = [k.strip() for k in fid.readlines()]
        keylist = [keylist_raw[i % len(keylist_raw)] for i in range(number)]

    fut = pw.map(run_command, keylist)
    res = pw.get_result(fut)

    f = open(outfile, 'wb', buffering=0)
    pickle.dump(res, f)
    f.flush()
    f.close()