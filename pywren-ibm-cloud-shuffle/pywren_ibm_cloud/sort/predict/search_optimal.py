import math
import os
import pickle
from os.path import expanduser
import numpy as np


# Minimum worker memory percentage to consider.
from pywren_ibm_cloud.sort.config import DEFAULT_GRANULE_SIZE_SHUFFLE
from pywren_ibm_cloud.sort.predict.get_functions import PYWREN_DIRNAME, PREDICTION_DATA_FILENAME

MINIMUM_WORKER_MEMORY_PERC = 0.065
# Maximum worker memory percentage to consider.
MAXIMUM_WORKER_MEMORY_PERC = 0.4
WORKER_NUMBER_STEP = 1

def search_optimal(config, dataset_size, granularity=None):

    # Dataset size in bytes.
    # Worker memory in Megabytes.
    worker_memory = config['pywren']['runtime_memory']
    if granularity is None:
        granularity = DEFAULT_GRANULE_SIZE_SHUFFLE

    # We establish the minimum and maximum worker number
    # to analyze as multiples of the worker number
    # step.
    minimum_number_worker = int( ( dataset_size / \
                            ( worker_memory * 1024**2
                              * MAXIMUM_WORKER_MEMORY_PERC ) )
                                 / WORKER_NUMBER_STEP ) \
                            * WORKER_NUMBER_STEP
    minimum_number_worker = max(minimum_number_worker, 3)
    maximum_number_worker = int ( ( dataset_size / \
                            ( worker_memory * 1024**2
                              * MINIMUM_WORKER_MEMORY_PERC ) )
                                  / WORKER_NUMBER_STEP )\
                            * WORKER_NUMBER_STEP
    maximum_number_worker = max(min(maximum_number_worker, 1000), 3)

    home = expanduser("~")
    # Check if equation info exists
    if not os.path.isfile("{}/{}/{}_{}.pickle".format(home, PYWREN_DIRNAME,
                                PREDICTION_DATA_FILENAME, granularity)):
        print("There is no parameter file for the worker number inferencer.")
        print("Execute setup_shuffle_model to configurate the system.")
        exit(-1)

    # Load equation info
    with open("{}/{}/{}_{}.pickle".format(home, PYWREN_DIRNAME,
                                PREDICTION_DATA_FILENAME, granularity),
              'rb') as f:
        equation_info = pickle.load(f)

    print(equation_info)
    bandwidth_read = equation_info['bandwidth']['read']
    bandwidth_write = equation_info['bandwidth']['write']
    throughput_read = equation_info['throughput']['read']
    throughput_write = equation_info['throughput']['write']

    # Apply Locus equations to each worker number
    predicted_times = {
        w:eq(dataset_size, w, bandwidth_read, bandwidth_write, throughput_read, throughput_write)
        for w in range(minimum_number_worker,
                   maximum_number_worker+1,
                   WORKER_NUMBER_STEP)
    }
    optimal_worker_number = min(predicted_times, key=predicted_times.get)

    return optimal_worker_number


def eq(D, p, bandwidth_read, bandwidth_write, throughput_read, throughput_write):
    return max(D/(bandwidth_read*p), np.ceil(D/(64.0*p))*p/throughput_read) + \
           max(D/(bandwidth_write*p), np.ceil(D/(64.0*p**2))*p**2/throughput_write) + \
           max(D/(bandwidth_read*p), np.ceil(D/(64.0*p**2))*p**2/throughput_read) + \
           max(D/(bandwidth_write*p), np.ceil(D/(64.0*p))*p/throughput_write)
