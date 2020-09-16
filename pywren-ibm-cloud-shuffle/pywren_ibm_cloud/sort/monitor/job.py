import os
import pickle
from pywren_ibm_cloud import utils
from pywren_ibm_cloud.job.job import _create_job
from pywren_ibm_cloud.sort.monitor.config import BACKUP_INFO_PATH, BACKUP_INFO_KEY, COMMUNICATION_PREFIX, FUTURE_PREFIX, \
    BASEPATH_PREFIX, END_PREFIX, START_PREFIX, MONITOR_INFO_PATH
from pywren_ibm_cloud.config import EXECUTION_TIMEOUT, MAX_AGG_DATA_SIZE, JOBS_PREFIX
from pywren_ibm_cloud.sort.monitor import monitor
from pywren_ibm_cloud import version


def create_monitor_task(config, internal_storage, executor_id, my_job_id, job_orchestrated, task_iterdata,
                        orchestration_functions, exec,
                        runtime_meta, runtime_memory=None, extra_params=None, extra_env=None, obj_chunk_size=None,
                        invoke_pool_threads=128, include_modules=[], exclude_modules=[],
                        execution_timeout=EXECUTION_TIMEOUT, speculative=False):
    """
    Wrapper to create a backup job.
    """
    # backup_func = backup.backup_task
    monitor_func = monitor.monitor_task

    if 'compute_backend' not in config['pywren'] or config['pywren']['compute_backend'] == 'ibm_cf':
        runtime_timeout_default = execution_timeout
    else:
        # TODO: Timeout for each compute backend
        runtime_timeout_default = 600

    futures_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id, FUTURE_PREFIX)
    communication_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id, COMMUNICATION_PREFIX)
    end_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id, END_PREFIX)
    start_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id, START_PREFIX)
    base_path = '{}_{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id)
    monitor_info_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, executor_id, my_job_id, MONITOR_INFO_PATH)


    static_monitor_args = [{'monitor_static_info': {'task_ids': orchestration_functions,
                                                   'bucket': config['pywren']['storage_bucket'],
                                                   'backup_info_key': BACKUP_INFO_KEY,
                                                   'monitor_info_path': BACKUP_INFO_PATH,
                                                   'communication_path': communication_path,
                                                   'futures_path': futures_path,
                                                   'end_path': end_path,
                                                   'start_path': start_path,
                                                   'monitor_info_path': monitor_info_path,
                                                   'base_path': base_path,
                                                   'config': config,
                                                    'job_id': my_job_id,
                                                    'job_orchestrated': job_orchestrated,
                                                    'log_level': os.getenv('PYWREN_LOGLEVEL'),
                                                    'pywren_version': version.__version__,
                                                    'speculative': speculative,
                                                    'runtime_timeout_default': runtime_timeout_default }}]

    map_iterdata = utils.verify_args(monitor_func, static_monitor_args, extra_params)
    new_invoke_pool_threads = invoke_pool_threads
    new_runtime_memory = runtime_memory

    job_description = _create_job(config, internal_storage, executor_id,
                                  my_job_id, monitor_func, map_iterdata,
                                  runtime_meta=runtime_meta,
                                  runtime_memory=new_runtime_memory,
                                  extra_env=extra_env,
                                  invoke_pool_threads=new_invoke_pool_threads,
                                  include_modules=include_modules,
                                  exclude_modules=exclude_modules,
                                  execution_timeout=execution_timeout)

    # Upload backup info (pickled) for backup function recursivity
    dynamic_backup_args = {
        'current_recursion': 0,
        'job_description': job_description
    }

    print("Uploading iterdata")

    # for idx, data in enumerate(task_iterdata):
    #     internal_storage.put_data(key="{}{}/{}".format(job_orchestrated['job_id'], DEFAULT_ARGUMENT_PATH, orchestration_functions[idx]),
    #                               data=pickle.dumps(data))

    internal_storage.put_data(key="{}/{}".format(monitor_info_path,
                                                 BACKUP_INFO_KEY),
                              data=pickle.dumps(dynamic_backup_args))

    return job_description
