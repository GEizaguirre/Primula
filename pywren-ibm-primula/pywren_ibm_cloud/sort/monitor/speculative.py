import os
import time

import pickle
from collections import deque

from pywren_ibm_cloud.config import extract_storage_config
from pywren_ibm_cloud.future import ResponseFuture
from pywren_ibm_cloud.sort.monitor.estimator import DefaultIBMEstimator
from pywren_ibm_cloud.sort.monitor.progress_calculator import DefaultIBMProgressCalculator
from pywren_ibm_cloud.sort.monitor.config import DEFAULT_SPECULATIVE_MAXIMUM_ALLOWED_TASKS, DEFAULT_SPECULATIVECAP_TOTAL_TASKS, \
    SPECULATION_REFERENCE_QUANTILE, SPECULATION_PROPORTION_REFERENCE, MINIMUM_PROCESSED_FUNCTIONS_TO_SPECULATE, SPECULATIVE_SLOWTASK_THRESHOLD, MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASKS, \
    MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASK_PROPORTION, MONITOR_FINISH_ALL_TASKS_FINISHED, \
    MONITOR_FINISH_NOT_ENOUGH_FUNCTION_NUMBER, MINIMUM_NUMBER_FUNCTIONS_TO_SPECULATE
import numpy as np


class Speculator:

    def __init__(self, monitor_static_info, monitor_dynamic_info, ibm_cos, percentile=None):

        self.ibm_cos = ibm_cos
        self.bucket = monitor_static_info['bucket']
        self.job_orchestrated = monitor_static_info['job_orchestrated']
        self.monitor_job_id = monitor_static_info['job_id']
        self.config = monitor_static_info['config']
        self.task_ids = monitor_static_info['task_ids']
        self.task_indexes = list(range(len(monitor_static_info['task_ids'])))
        self.storage_config = extract_storage_config(self.config)
        self.log_level = monitor_static_info['log_level']
        self.pywren_version = monitor_static_info['pywren_version']
        self.futures_path = monitor_static_info['futures_path']
        self.communication_path = monitor_static_info['communication_path']
        if percentile is None:
            self.percentile = SPECULATION_REFERENCE_QUANTILE
        else:
            self.percentile = percentile

        # Speculation data structures
        # self.may_have_speculated = []
        # self.running_task_attempt_statistics = {}
        # self.task_attemps =  {}
        # self._initialize_attempts()
        # self.speculative_counter = 0
        self.speculated_indexes = []
        self.indexes_currently_in_speculator = []
        self.finished_indexes = []
        self.indexes_to_consider = list(range(len(monitor_static_info['task_ids'])))
        print("indexes to consider {}".format(self.indexes_to_consider))
        self.threshold_runtime = SPECULATIVE_SLOWTASK_THRESHOLD
        self.last_task_index_stack = deque()
        self.last_task_runtime = SPECULATIVE_SLOWTASK_THRESHOLD

        # Progress estimator
        self.estimator = DefaultIBMEstimator(self.task_indexes)

        if 'progress_calculator' not in monitor_static_info:
            self.progress_calculator = DefaultIBMProgressCalculator()

        # TODO: delete this
        print("Using speculation percentile {}".format(SPECULATION_REFERENCE_QUANTILE))

    def _compute_speculations(self, compute_handler):

        speculated_index = None

        print("computing speculations")

        runtime_estimations, progress_scores = self._get_runtime_estimations()
        # print("runtime estimations: {}".format(runtime_estimations))

        if len(runtime_estimations) is not 0 and \
                len(self.indexes_currently_in_speculator) > \
                len(self.task_indexes) * MINIMUM_PROCESSED_FUNCTIONS_TO_SPECULATE:

            # print("Last index stack {}".format(self.last_task_index_stack))
            if len(self.last_task_index_stack) > 0:
                self.threshold_runtime = self.estimator.threshold_runtime(self.last_task_runtime)
            else:
                self.threshold_runtime = SPECULATIVE_SLOWTASK_THRESHOLD
            # print("threshold runtime {}".format(self.threshold_runtime))

            speculated_index = self._speculate(runtime_estimations, progress_scores)
            if speculated_index is not None:
                print("speculated index {}".format(speculated_index))
                print("indexes to consider {}".format(self.indexes_to_consider))
                self.indexes_to_consider.remove(int(speculated_index))
                self.speculated_indexes.append(speculated_index)
                # Find next newest task.
                while len(self.last_task_index_stack) > 0 \
                        and self.last_task_index_stack[-1] not in self.indexes_to_consider:
                    self.last_task_index_stack.pop()
                self._launch_speculative(int(speculated_index), compute_handler)

        # print("speculated indexes {}".format(self.speculated_indexes))
        # print("task indexes {}".format(self.task_indexes))
        # print("finished indexes {}".format(self.finished_indexes))

        is_end = self._check_end()

        return speculated_index, is_end

    def _check_end(self):
        if len(self.speculated_indexes) > DEFAULT_SPECULATIVE_MAXIMUM_ALLOWED_TASKS:
            return MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASKS
        if len(self.speculated_indexes) >= len(self.task_indexes) * DEFAULT_SPECULATIVECAP_TOTAL_TASKS:
            if self.speculated_indexes is 0 and len(self.finished_indexes) is not len(self.task_indexes):
                return 0
            else:
                return MONITOR_FINISH_MAXIMUM_SPECULATIVE_TASK_PROPORTION
        if len(self.finished_indexes) == len(self.task_indexes):
            return MONITOR_FINISH_ALL_TASKS_FINISHED
        if (len(self.task_indexes) - len(self.finished_indexes) - len(self.speculated_indexes)) < (
                len(self.task_indexes) * MINIMUM_PROCESSED_FUNCTIONS_TO_SPECULATE):
            if self.speculated_indexes is 0 and len(self.finished_indexes) is not len(self.task_indexes):
                return 0
            else:
                return MONITOR_FINISH_NOT_ENOUGH_FUNCTION_NUMBER
        if (len(self.task_indexes) - len(self.finished_indexes) - len(self.speculated_indexes)) < \
                MINIMUM_NUMBER_FUNCTIONS_TO_SPECULATE:
            if self.speculated_indexes is 0 and len(self.finished_indexes) is not len(self.task_indexes):
                return 0
            else:
                return MONITOR_FINISH_NOT_ENOUGH_FUNCTION_NUMBER
        return 0

    def _get_runtime_estimations(self):

        # print("Extracting runtime estimations, from {}".format(self.communication_path))

        runtime_estimations = {}
        progress_scores = {}

        task_objects = self.ibm_cos.list_objects_v2(Bucket=self.bucket, Prefix=self.communication_path)
        # print(task_objects)

        if 'Contents' in task_objects.keys():
            tasks_progress_information = [o['Key'] for o in task_objects['Contents']]
        else:
            return runtime_estimations, progress_scores

        # print(tasks_progress_information)
        # print("indexes processed {}".format(self.indexes_currently_in_speculator))

        # print("progres info objects")
        # print(tasks_progress_information)

        for task_data in tasks_progress_information:

            # print("Extracting info {}".format(task_data))

            task_index = int(os.path.splitext(os.path.basename(task_data))[0])

            # print("analizing task idnex {}".format(task_index))

            if task_index in self.task_indexes and task_index in self.indexes_to_consider:

                task_info = pickle.loads(self.ibm_cos.get_object(Bucket=self.bucket, Key=task_data)['Body'].read())
                # print(task_info)

                task_estimated_runtime, task_finished = self.estimator.estimated_runtime(task_info)
                # print("taking {} info: estimated runtime {} ( task info {} )".format(task_index, task_estimated_runtime, task_info))

                # Check if task has ended
                if task_finished:
                    print("Finished task {}".format(task_index))
                    self.ibm_cos.delete_object(Bucket=self.bucket, Key=task_data)
                    self.indexes_to_consider.remove(task_index)
                    self.finished_indexes.append(task_index)
                    # Find next newest task.
                    while len(self.last_task_index_stack) > 0 \
                            and self.last_task_index_stack[-1] not in self.indexes_to_consider:
                        self.last_task_index_stack.pop()
                else:
                    # print("estimating runtime {}".format(task_index))
                    # print("task {} time {}, threshold {}".format(task_index, task_estimated_runtime, self.threshold_runtime))
                    if task_info['time'] >= self.threshold_runtime:
                        runtime_estimations[str(task_index)] = task_estimated_runtime
                        progress_scores[str(task_index)] = task_info['progress']

                        # Get task which last uploaded info to COS (the one that last started)
                        if task_index not in self.indexes_currently_in_speculator:
                            self.last_task_index_stack.append(task_index)
                            self.indexes_currently_in_speculator.append(task_index)

                    if len(self.last_task_index_stack) > 0 and task_index == self.last_task_index_stack[-1]:
                        self.last_task_runtime = task_info['time']

        return runtime_estimations, progress_scores

    def _speculate(self, runtime_estimations, progress_scores):

        # print("speculating index")
        # Sort runtimes
        runtime_info_sorted = {k: v for k, v in reversed(sorted(runtime_estimations.items(), key=lambda item: item[1]))}
        progress_scores_sorted = {k: v for k, v in sorted(progress_scores.items(), key=lambda item: item[1])}
        print("Speculation tests")
        print("runtime estimations {}".format(runtime_info_sorted))
        print("progress scores {}".format(progress_scores_sorted))
        print("threshold time {}".format(self.threshold_runtime))
        progress_scores_sorted_values = list(progress_scores_sorted.values())

        # print("getting quantile from {}".format(runtime_info_sorted))

        # Get reference quantile
        reference_progress_score = np.quantile(progress_scores_sorted_values, self.percentile)
        print("reference progress score {}".format(reference_progress_score))

        slowest_key = None
        # Get first task with execution time among threshold
        for k, v in runtime_info_sorted.items():
            if progress_scores_sorted[k] < reference_progress_score * SPECULATION_PROPORTION_REFERENCE:
                slowest_key = k
                break

        return slowest_key

    def _launch_speculative(self, task_index, compute_handler):

        # the first call id for this job is the same monitor task
        call_id = "{:05d}".format(task_index + 1)
        # speculative_job_id = _create_speculative_job_id('S', backup_static_info)

        # Call new function compute handler.
        payload = {'config': self.config,
                   'log_level': self.log_level,
                   'func_key': self.job_orchestrated['func_key'],
                   'data_key': self.job_orchestrated['data_key'],
                   'extra_env': self.job_orchestrated['extra_env'],
                   'execution_timeout': self.job_orchestrated['execution_timeout'],
                   'data_byte_range': self.job_orchestrated['data_ranges'][task_index],
                   'executor_id': self.job_orchestrated['executor_id'],
                   'job_id': self.monitor_job_id,
                   'call_id': call_id,
                   'host_submit_time': time.time(),
                   'pywren_version': self.pywren_version}

        compute_handler.invoke(self.job_orchestrated['runtime_name'],
                               self.job_orchestrated['runtime_memory'], payload)

        future = ResponseFuture(self.job_orchestrated['executor_id'], self.monitor_job_id, call_id,
                                self.storage_config, self.job_orchestrated['execution_timeout'],
                                self.job_orchestrated['metadata'])

        future._set_state(ResponseFuture.State.Invoked)

        self.ibm_cos.put_object(Bucket=self.bucket, Key="{}/{}.pickle".format(self.futures_path, task_index),
                                Body=pickle.dumps(future))

# def _initialize_attempts (self):
#    for idx, task_id in self.task_ids:
#        self.task_attemps['task_id'] = { 'task_id': task_id, 'id': task_id, 'state': ATTEMPT_RUNNING,
#                                      'start_time': 0, 'spec': False, 'future': None }

# # TODO
# def _get_tasks(self, backup_static_info):
#     # Return task ids
#     return 1
#
# def _get_task_attempts(self, task_id):
#     # Get each attempt's data from IBM COS
#     attempts_data = self.ibm_cos.list_objects_v2(Bucket=self.bucket, Prefix="{}/{}".format(self.orchestration_path, task_id))
#
#     # get progress for each object
#     # will transer to estimator statistics the task with less time left to finish
#     aux_task = { 'id': task_id, 'total_time': RUNTIME_MAX_VALUE }
#     for attempt_name in attempts_data:
#         attempt_id = os.path.basename(attempt_name)
#         attempt_data = pickle.dumps(self.ibm_cos.get_object(Bucket=self.bucket, Key=attempt_name))
#         self.task_attemps[attempt_id]['progress'] = self.progress_calculator.get_progress_score(attempt_data)
#         self.task_attemps[attempt_id]['state'] = attempt_data['state']
#         self.task_attemps[attempt_id]['total_time'] = attempt_data['total_time']
#         if attempts_data['total_time'] < aux_task['total_time']:
#             aux_task['total_time'] = attempt_data['total_time']
#
#     # HERE
#     # self.estimator.
#
#     return [value for key, value in self.task_attemps.iteritems() if key.startswith(task_id)]
#
#
# def _speculation_value(self, task, current_time):
#     attempts = self._get_task_attempts(task['id'])
#     acceptable_runtime = -1
#     result = -NUM_TASK_STATES - 1
#
#     if task['id'] not in self.may_have_speculated:
#         acceptable_runtime = self.estimator.threshold_runtime(task)
#         if acceptable_runtime == RUNTIME_MAX_VALUE:
#             return ON_SCHEDULE
#
#     running_task_attempt_id = None
#     number_running_attemps = 0
#
#     for attempt in attempts:
#         if attempt['state'] is ATTEMPT_RUNNING or attempt['state'] is ATTEMPT_STARTING:
#             if number_running_attemps > 0:
#                 return ALREADY_SPECULATING
#             number_running_attemps += 1
#             attempt_id = attempt['id']
#
#             estimated_run_time = self.estimator.estimatedRuntime(attempt)
#
#             task_attempt_start_time = attempt['start_time']
#             # too early to evaluate the task
#             if task_attempt_start_time > current_time:
#                 return TOO_NEW
#
#             estimated_end_time = estimated_run_time + task_attempt_start_time
#
#             # TODO
#             # estimated_replacement_end_time = current_time \
#             #                                  + self.estimator.estimated_new_attempt_runtime(task)
#
#             # Calculated with our algorithm
#             progress = attempt['progress']
#
#             # if attempt['id'] not in self.running_task_attempt_statistics:
#             #     self.running_task_attempt_statistics[attempt['id']] = TaskAttemptHistoryStatistics(
#             #         estimated_end_time,
#             #         progress,
#             #         current_time)
#             # else:
#             #     data = self.running_task_attempt_statistics[attempt['id']]
#
#                 # if estimated_run_time == data.estimated_run_time and progress == data.progress:
#                 #     # TODO
#                 #     if data.not_heartbeated_in_a_while(current_time) or self.estimator.has_stagnated_progress(
#                 #             attempt, current_time):
#                 #         # TODO
#                 #         self._handle_attempt(attempt['id'], progress, attempt['state'])
#                 # else:
#                 #     self.running_task_attempt_statistics[attempt['id']].update(estimated_run_time,
#                 #                                                                progress,
#                 #                                                                current_time)
#
#             if estimated_end_time < current_time:
#                 return PROGRESS_IS_GOOD
#
#             # HERE
#             # if estimated_replacement_end_time >= estimated_end_time:
#             #    return TOO_LATE_TO_SPECULATE
#
#             # result = estimated_end_time - estimated_replacement_end_time
#
#     if number_running_attemps == 0:
#         return NOT_RUNNING
#
#     if acceptable_runtime == -1:
#         # TODO
#         acceptable_runtime = self.estimator.threshold_runtime(task['id'])
#         if acceptable_runtime == RUNTIME_MAX_VALUE:
#             return ON_SCHEDULE
#
#     return result
#
# def _add_speculative_attempt(self, task_id, task_number, current_time):
#
#     call_id = "{:05d}".format(self.speculative_counter)
#
#     # Call new function compute handler.
#     payload = {'config': self.config,
#                'log_level': self.log_level,
#                'func_key': self.job_orchestrated['func_key'],
#                'data_key': self.job_orchestrated['data_key'],
#                'extra_env': self.job_orchestrated['extra_env'],
#                'execution_timeout': self.job_orchestrated['execution_timeout'],
#                'data_byte_range': self.job_orchestrated['data_ranges'][task_number],
#                'executor_id': self.job_orchestrated['executor_id'],
#                'job_id': self.monitor_job_id,
#                'call_id': call_id,
#                'host_submit_time': time.time(),
#                'pywren_version': self.pywren_version}
#     self.compute_handler.invoke(self.job_orchestrated['runtime_name'], self.job_orchestrated['runtime_memory'], payload)
#
#     # Save speculated task id into local structure.
#     self.may_have_speculated.append(task_id)
#     num_attempt = len([value for key, value in self.task_attemps.iteritems() if key.startswith(task_id)])
#     attempt_id = "{}_{}".format(task_id, num_attempt)
#
#     future = ResponseFuture (self.job_orchestrated['executor_id'], self.monitor_job_id, call_id, self.storage_config,
#                              self.job_orchestrated['execution_timeout'], self.job_orchestrated['metadata'])
#     future._set_state(ResponseFuture.State.Invoked)
#
#     self.task_attemps[attempt_id] = { 'task_id': task_id, 'id': attempt_id, 'state':ATTEMPT_STARTING,
#                                       'start_time': current_time, 'spec': True, 'future': future }
#     self.speculative_counter += 1
#
# def _compute_speculations(self):
#
#     successes = 0
#     number_speculations_already = 0
#     number_running_tasks = 0
#
#     current_time = time.time()
#
#     tasks = self._get_tasks()
#
#     number_allowed_speculative_tasks = max(DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS,
#                                            DEFAULT_SPECULATIVECAP_TOTAL_TASKS * len(tasks))
#
#     best_task_id = None
#     best_speculation_value = -1
#
#     # TODO: track tasks that are potentially worth looking at
#     for idx, task in enumerate(tasks):
#
#         my_speculation_value = self._speculation_value(task, current_time)
#
#         # Only consider negative values, as _speculation_value could return
#         # a few magic numbers.
#         if my_speculation_value < 0:
#             if my_speculation_value is ALREADY_SPECULATING:
#                 number_speculations_already += 1
#             if my_speculation_value is not NOT_RUNNING:
#                 number_running_tasks += 1
#             if my_speculation_value > best_speculation_value:
#                 best_task_id = task['id']
#                 task_number = idx
#                 best_speculation_value = my_speculation_value
#
#     number_allowed_speculative_tasks = max(DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS,
#                                            DEFAULT_SPECULATIVECAP_TOTAL_TASKS * number_running_tasks)
#
#     if best_task_id is not None and number_allowed_speculative_tasks > number_speculations_already:
#         # TODO
#         self._add_speculative_attempt(best_task_id, task_number, current_time)
#         successes += 1
#
#     return successes
#
# def _handle_attempt(self, attempt_id, progress, attempt_state):
#     return 1
