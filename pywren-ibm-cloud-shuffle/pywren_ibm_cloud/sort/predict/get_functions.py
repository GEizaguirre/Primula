import os
import pickle
import time
from os.path import expanduser

from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE
from pywren_ibm_cloud.sort.predict.cos_throughput_test import write_throughput, read_throughput
from pywren_ibm_cloud.sort.predict.cos_bandwidth_test import write, read
import numpy as np

# Number of workers for throughput evaluation.
THROUGHPUT_EVALUATION_WORKERS = 15
# Operations per worker for throughput evaluation.
OPERATIONS_PER_WORKER = 10
# Worker numbers for bandwidth evaluation.
BANDWIDTH_EVALUATION_WORKERS = [10,250,500,1000]
THROUGHPUT_EVALUATION_WORKERS = [10, 200, 500, 750]
DEFAULT_PREFIX = "cme_"

PYWREN_DIRNAME = ".pywren"
PREDICTION_DATA_FILENAME = "cos_prediction_data"
KEY_FILE = "key_file"


def get_cos_functions(config, granularity=None):

    time_start = time.time()

    if granularity is None:
        granularity = DEFAULT_GRANULE_SIZE_SHUFFLE

    # Get COS throughput data
    print("Performing throughput evaluations")
    for o in THROUGHPUT_EVALUATION_WORKERS:
        print("{} workers".format(o))
        write_throughput(config, config["pywren"]["storage_bucket"],
                         granularity, o, OPERATIONS_PER_WORKER, DEFAULT_PREFIX,
                         "cos_benchmark_throughput_w{}_o{}.write.output.pickle"
                         .format(o, OPERATIONS_PER_WORKER), KEY_FILE)
        read_throughput(config, config["pywren"]["storage_bucket"],
                        granularity, o, OPERATIONS_PER_WORKER, DEFAULT_PREFIX,
                        "cos_benchmark_throughput_w{}_o{}.read.output.pickle"
                        .format(o, OPERATIONS_PER_WORKER), KEY_FILE)

    # Get COS bandwidth data
    print("Performing bandwidth evaluations")
    for n in BANDWIDTH_EVALUATION_WORKERS:
        print("{} workers".format(n))
        write(config, config["pywren"]["storage_bucket"], granularity, n, DEFAULT_PREFIX,
              "cos_benchmark_bandwidth.write.output_g{}_w{}.pickle"
              .format(granularity, n), KEY_FILE)
        read(config, config["pywren"]["storage_bucket"], n,
             "cos_benchmark_bandwidth.read.output_g{}_w{}.pickle"
             .format(granularity, n),
             KEY_FILE, None, 1, granularity)

    os.remove(KEY_FILE)

    print("Calculating values")

    # Get throughput read function
    agg_throughputs = np.array([], dtype='float64')
    for o in THROUGHPUT_EVALUATION_WORKERS:
        file_name = "cos_benchmark_throughput_w{}_o{}.read.output.pickle" \
            .format(o, OPERATIONS_PER_WORKER)
        with open(file_name, 'rb') as f:
            n = np.array(pickle.load(f))[:, 2]
            # Aggregate throughput is the sum of every function throughput.
            agg_throughputs = np.append(agg_throughputs, np.sum(n))
        os.remove(file_name)
    max_throughput_read = max(agg_throughputs)

    # Get throughput write function
    agg_throughputs = np.array([], dtype='float64')
    for o in THROUGHPUT_EVALUATION_WORKERS:
        file_name = "cos_benchmark_throughput_w{}_o{}.write.output.pickle" \
            .format(o, OPERATIONS_PER_WORKER)
        with open(file_name, 'rb') as f:
            n = np.array(pickle.load(f))[:, 2]
            # Aggregate throughput is the sum of every function throughput.
            agg_throughputs = np.append(agg_throughputs, np.sum(n))
        os.remove(file_name)
    max_throughput_write = max(agg_throughputs)

    # Get bandwidth read function
    agg_bandwidths = np.array([], dtype='float64')
    for w in BANDWIDTH_EVALUATION_WORKERS:
        file_name = "cos_benchmark_bandwidth.read.output_g{}_w{}.pickle" \
            .format(granularity, w)
        with open(file_name, 'rb') as f:
            n = np.array(pickle.load(f))[:, 2]
            # Aggregate throughput is the sum of every function throughput.
            agg_bandwidths = np.append(agg_bandwidths, np.sum(n))
        os.remove(file_name)
    max_bandwidth_read = max(agg_bandwidths)


    # Get bandwidth write function
    agg_bandwidths = np.array([], dtype='float64')
    for w in BANDWIDTH_EVALUATION_WORKERS:
        file_name = "cos_benchmark_bandwidth.write.output_g{}_w{}.pickle" \
            .format(granularity, w)
        with open(file_name, 'rb') as f:
            n = np.array(pickle.load(f))[:, 2]
            # Aggregate throughput is the sum of every function throughput.
            agg_bandwidths = np.append(agg_bandwidths, np.sum(n))
        # os.remove(file_name)
    max_bandwidth_write = max(agg_bandwidths)

    # Save information in pywren directory.
    equation_info = {'throughput': {'read': max_throughput_read,
                                    'write': max_throughput_write},
                     'bandwidth': {'read': max_bandwidth_read/1024,
                                   'write': max_bandwidth_write/1024}}

    print("Seup results {}".format(equation_info))
    print("setup time %.3f"%(time.time()-time_start))

    home = expanduser("~")
    if not os.path.isdir("{}/{}".format(home, PYWREN_DIRNAME)):
        os.mkdir("{}/{}".format(home, PYWREN_DIRNAME))
    print("Saving at {}/{}/{}_{}.pickle".format(home, PYWREN_DIRNAME,
                                PREDICTION_DATA_FILENAME, granularity))
    with open("{}/{}/{}_{}.pickle".format(home, PYWREN_DIRNAME,
                                PREDICTION_DATA_FILENAME, granularity),
              'wb') as f:
        pickle.dump(equation_info, f)

    return DEFAULT_PREFIX
