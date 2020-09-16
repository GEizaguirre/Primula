import time
import pickle
import logging

from ibm_botocore.exceptions import ClientError
from pywren_ibm_cloud.future import ResponseFuture
from pywren_ibm_cloud.sort.monitor.config import SOONEST_RETRY_AFTER_SPECULATE, SOONEST_RETRY_AFTER_NO_SPECULATE, \
    MAX_BACKUP_FUNCTION_NUMBER, FUTURE_PREFIX, TIMEOUT_MARGIN, BASEPATH_PREFIX, MONITOR_FINISH_TIMEOUT, \
    MONITOR_FINISH_NOT_ENOUGH_FUNCTION_NUMBER, MONITOR_FINISH_NOT_ENOUGH_FUNCTION_PROPORTION, \
    MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASKS, MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASK_PROPORTION, \
    MONITOR_FINISH_ALL_TASKS_FINISHED, MONITOR_TIMEOUT, MONITOR_FINISH_MAXIMUM_RECURSION, \
    MONITOR_FINISH_SIGNALED_FROM_CLIENT, SPECULATION_START_TIME, SPECULATION_REFERENCE_QUANTILE
from pywren_ibm_cloud.sort.monitor.speculative import Speculator
from pywren_ibm_cloud.config import EXECUTION_TIMEOUT, extract_storage_config
from pywren_ibm_cloud.compute import Compute
from pywren_ibm_cloud.config import extract_compute_config

logger = logging.getLogger(__name__)


# TODO: Finish algorithm implementation.

# TODO: When calling map with backup, save map function's job_description in orchestrator path for
# posterior invocation of replicas.

# TODO: Modify mappers to write into COS progress files periodically.

# TODO: Add backup function's future to map's futures.
# TODO: At the end of backup function, it must replace real output directories with replicas', as they could
# finish faster than the original functions.

def monitor_task(ibm_cos, monitor_static_info):

    monitor_start_time = time.time()
    speculative = monitor_static_info['speculative']
    bucket = monitor_static_info['bucket']
    communication_path = monitor_static_info['communication_path']
    start_path = monitor_static_info['start_path']
    end_path = monitor_static_info['end_path']
    futures_path = monitor_static_info['futures_path']
    monitor_info_path = monitor_static_info['monitor_info_path']
    base_path = monitor_static_info['base_path']
    monitor_job_id = monitor_static_info['job_id']

    if "start_time" in monitor_static_info:
        start_time = monitor_static_info['start_time']
    else:
        start_time = SPECULATION_START_TIME

    if "percentile" in monitor_static_info:
        percentile = monitor_static_info['percentile']
    else:
        percentile = SPECULATION_REFERENCE_QUANTILE

    # Read backup info for recursions
    try:
        monitor_dynamic_info = pickle.loads(ibm_cos.get_object(Bucket=monitor_static_info['bucket'],
                                                               Key="{}/{}".format(monitor_info_path,
                                                                                    monitor_static_info[
                                                                                        'backup_info_key']))[
                                               'Body'].read())
        print("current recursion {}".format(monitor_dynamic_info['current_recursion']))
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print('Backup worker did not find dynamic data')
        else:
            raise

    # Structure intialization
    if monitor_dynamic_info['current_recursion'] is 0:
        monitor_dynamic_info['total_monitor_runtime'] = 0
        if monitor_static_info['speculative']:
            # Initialize speculator
            print("Initializing default speculator")
            monitor_dynamic_info['speculator'] = Speculator(monitor_static_info,
                                                            monitor_dynamic_info,
                                                            ibm_cos,
                                                            percentile=percentile)

    # Prepare client for function calls.
    compute_handler = _get_compute_backend_client(monitor_static_info)
    print("Compute backend client created")

    print("start time {}, percentile {}".format(start_time, percentile))
    # return 0

    monitor_end = 0

    # Remove previous data in speculation task
    if speculative:
        for key in ibm_cos.list_objects_v2(Bucket=bucket, Prefix=base_path):
            ibm_cos.delete_object(Bucket=bucket, Key=key)
        print("Removed previous data from COS")
        print("Initial sleep")
        time.sleep(start_time)




    # time.sleep(5)
    sleep_time = SOONEST_RETRY_AFTER_NO_SPECULATE

    # Initial sleep

    while monitor_end is 0 :

        # print("loop")
        # Check monitor end signal
        monitor_end = _check_monitor_end(ibm_cos, bucket, end_path)
        if monitor_end is not 0:
            print("signaled end")
            break

        if monitor_static_info['speculative']:

             speculation_index, monitor_end = monitor_dynamic_info['speculator']._compute_speculations(compute_handler)

             if speculation_index is not None:
                 sleep_time = SOONEST_RETRY_AFTER_SPECULATE
                 print("Called speculative task {} at {} from start".format(speculation_index,
                                                                            time.time() - monitor_start_time))
             else:
                 sleep_time = SOONEST_RETRY_AFTER_NO_SPECULATE

        current_monitor_runtime = time.time() - monitor_start_time
        if monitor_dynamic_info['total_monitor_runtime'] + current_monitor_runtime > MONITOR_TIMEOUT:
            monitor_end = MONITOR_FINISH_TIMEOUT
            break
        if (current_monitor_runtime) > (EXECUTION_TIMEOUT - TIMEOUT_MARGIN):
            break
        else:
            # print("Sleeping {} seconds".format(sleep_time))
            time.sleep(sleep_time)

    if monitor_end is not 0:
        _print_end_message(monitor_end)
    else:
        if monitor_dynamic_info['current_recursion'] < (MAX_BACKUP_FUNCTION_NUMBER - 1):
            # Call child backup task.
            monitor_dynamic_info['total_monitor_runtime'] += time.time()-monitor_start_time
            runtime_name, runtime_memory, payload = _prepare_next_backup_task_execution(ibm_cos, monitor_static_info,
                                                                                        monitor_dynamic_info)
            print("invoking new backup task")
            compute_handler.invoke(runtime_name, runtime_memory, payload)
        else:
            _print_end_message(MONITOR_FINISH_TIMEOUT)
    print("Finishing monitor task")

def _check_monitor_end (ibm_cos, bucket, end_path):
    print("Checking montior end from {}".format("{}/monitor.pickle".format(end_path)))
    try:
        ibm_cos.get_object(Bucket=bucket,
                           Key="{}/monitor.pickle".format(end_path))
        return MONITOR_FINISH_SIGNALED_FROM_CLIENT
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return 0
        else:
            raise


def _get_compute_backend_client(backup_static_info):
    compute_config = extract_compute_config(backup_static_info['config'])
    return Compute(compute_config)


def _prepare_next_backup_task_execution(ibm_cos, monitor_static_info, backup_dynamic_info):
    backup_dynamic_info['current_recursion'] += 1

    # Save dynamic info into COS.
    ibm_cos.put_object(Bucket=monitor_static_info['bucket'],
                       Key="{}/{}".format(monitor_static_info['monitor_info_path'],
                                          monitor_static_info[
                                            'backup_info_key']),
                       Body=pickle.dumps(backup_dynamic_info))

    print("Put updated info at {}/{}".format(monitor_static_info['bucket'],
                                             "{}_{}/{}".format(monitor_static_info['monitor_info_path'],
                                                               monitor_static_info['job_id'],
                                                               monitor_static_info['backup_info_key'])))

    job = backup_dynamic_info['job_description']

    call_id = "0"
    payload = {'config': monitor_static_info['config'],
               'log_level': monitor_static_info['log_level'],
               'func_key': job['func_key'],
               'data_key': job['data_key'],
               'extra_env': job['extra_env'],
               'execution_timeout': job['execution_timeout'],
               'data_byte_range': job['data_ranges'][int(call_id)],
               'executor_id': job['executor_id'],
               'job_id': job['job_id'],
               'call_id': call_id,
               'host_submit_time': time.time(),
               'pywren_version': monitor_static_info['pywren_version']}

    return job['runtime_name'], backup_dynamic_info['job_description']['runtime_memory'], payload

def _print_end_message(monitor_end):
    if monitor_end is MONITOR_FINISH_NOT_ENOUGH_FUNCTION_NUMBER:
        print("Not enough functions to speculate")
    if monitor_end is MONITOR_FINISH_NOT_ENOUGH_FUNCTION_PROPORTION:
        print("Not enough function proportion left from total to speculate")
    if monitor_end is MONITOR_FINISH_TIMEOUT:
        print("Monitor function timeout")
    if monitor_end is MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASKS:
        print("Monitor maximum numbe rof speculative tasks reached")
    if monitor_end is MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASK_PROPORTION:
        print("Monitor maximum speculated task proportion reached")
    if monitor_end is MONITOR_FINISH_ALL_TASKS_FINISHED:
        print("Monitor all tasks finished")
    if monitor_end is MONITOR_FINISH_MAXIMUM_RECURSION:
        print("Monitor maximum recursion reached")
    if monitor_end is MONITOR_FINISH_SIGNALED_FROM_CLIENT:
        print("Monitor end signaled from client")
    print("Monitor work ended")