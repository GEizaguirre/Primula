import os
import pickle
import time

from pywren_ibm_cloud.sort.asynchronous.config import ASYNCHRONOUS_COMPLETE_PERCENTAGE, WHAIT_WHEN_FINISHED, \
    MAX_PRINT_ITERATION
from pywren_ibm_cloud.compute.backends.ibm_cf.config import MAX_CONCURRENT_WORKERS
from pywren_ibm_cloud.wait import ALL_COMPLETED
from pywren_ibm_cloud.sort.monitor.config import FUTURE_PREFIX, COMMUNICATION_PREFIX, BASEPATH_PREFIX, END_PREFIX, \
    START_PREFIX, BACKUP_INFO_PATH
from pywren_ibm_cloud.wait.wait_storage import _wait_storage
from queue import Queue as threadPoolQueue

from pywren_ibm_cloud.sort.asynchronous.patch import patch_invoker_numbered_run, unpatch_invoker_numbered_run


def wait_storage_monitor(fs, internal_storage, invoker, phase2_job, speculative_phase1=True, speculative_phase2=True,
                         phase2_monitor_job=None,
                         monitor_func_pos=None, download_results=False,
                         throw_except=True, pbar=None, return_when=ALL_COMPLETED,
                         threadpool_size=128, wait_dur_sec=1):
    patch_invoker_numbered_run(invoker)

    num_tasks_phase1 = len(fs) - 1
    num_phase2 = phase2_job['total_calls']
    num_tasks_phase2 = int(phase2_job['total_calls'])
    phase2_tasks = range(int(phase2_job['total_calls']))
    running_tasks = num_tasks_phase1 + int(phase2_job['total_calls'])
    print("Monitoring {} tasks (speculative phase1 {}, speculative phase2 {})".format(num_tasks_phase1,
                                                                                      speculative_phase1,
                                                                                      speculative_phase2))
    # Invocation data
    zero_time = time.time()
    invocation_time_info = dict()
    invocation_time_info['phase1'] = dict()
    invocation_time_info['phase2'] = dict()
    for i in range(num_tasks_phase1):
        # invocation_time_info['phase1_{}'.format(i)] = {'start': 0}
        invocation_time_info['phase1'][i] = {'start': 0}
    # invocation_time_info['phase1_monitor'] = {'start': 0}
    mitigation_counter_map = 0
    mitigation_counter_reduce = 0
    speculative_counter_map = 0
    speculative_counter_reduce = 0
    effective_map_ft = []

    print("Reducer number: {}".format(phase2_job['total_calls']))

    phase2_to_invoke = threadPoolQueue(int(phase2_job['total_calls']))
    for i in phase2_tasks:
        phase2_to_invoke.put(i)

    phase1_monitor_finished = False
    phase2_monitor_finished = False

    # These are performance-related settings that we may eventually
    # want to expose to end users:
    MAX_DIRECT_QUERY_N = 64
    RETURN_EARLY_N = 32
    RANDOM_QUERY = False

    result_count = 0

    if monitor_func_pos is None:
        monitor_func_pos = num_tasks_phase1
    phase1_index_finished = []
    phase1_index_killed = []
    phase1_tasks_speculated = []
    phase1_faster_speculatives = {}
    phase1_valid_indexes = {}
    if speculative_phase1:
        phase1_futures_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, fs[monitor_func_pos].executor_id,
                                                   fs[monitor_func_pos].job_id,
                                                   FUTURE_PREFIX)
        phase1_communication_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, fs[monitor_func_pos].executor_id,
                                                         fs[monitor_func_pos].job_id, COMMUNICATION_PREFIX)
        phase1_end_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, fs[monitor_func_pos].executor_id,
                                               fs[monitor_func_pos].job_id,
                                               END_PREFIX)
        phase1_start_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, fs[monitor_func_pos].executor_id,
                                                 fs[monitor_func_pos].job_id,
                                                 START_PREFIX)
    phase2_futures = []
    phase2_index_finished = []
    phase2_index_killed = []
    phase2_tasks_speculated = []
    phase2_faster_speculatives = {}
    phase2_valid_indexes = {}
    if speculative_phase2:
        phase2_futures_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, phase2_monitor_job['executor_id'],
                                                   phase2_monitor_job['job_id'],
                                                   FUTURE_PREFIX)
        phase2_communication_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, phase2_monitor_job['executor_id'],
                                                         phase2_monitor_job['job_id'], COMMUNICATION_PREFIX)
        phase2_end_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, phase2_monitor_job['executor_id'],
                                               phase2_monitor_job['job_id'],
                                               END_PREFIX)
        phase2_start_path = '{}_{}/{}/{}'.format(BASEPATH_PREFIX, phase2_monitor_job['executor_id'],
                                                 phase2_monitor_job['job_id'],
                                                 START_PREFIX)
    phase2_w_started = False

    print_iteration = 0

    while len(phase2_index_finished) < num_phase2:

        if print_iteration == MAX_PRINT_ITERATION:
            if len(phase1_index_finished) > 0:
                if len(phase1_index_finished) == num_tasks_phase1:
                    print("finished phase 1")
                else:
                    print("finished phase1 {}".format(phase1_index_finished))
            if len(phase1_tasks_speculated) > 0 and len(phase1_index_finished) != num_tasks_phase1:
                print("speculated phase1 {}".format(phase1_tasks_speculated))
            if len(phase2_index_finished) > 0:
                print("finished phase2 {}".format(phase2_index_finished))
            if len(phase2_tasks_speculated) > 0:
                print("speculated phase2 {}".format(phase2_tasks_speculated))
            print_iteration = 0
        else:
            print_iteration += 1

        # Look for speculative map futures in COS
        if speculative_phase1:
            if len(phase1_tasks_speculated) < num_tasks_phase1:
                spec_futures, index_speculated = _check_speculative_futures(internal_storage, phase1_futures_path)
                if len(spec_futures) > 0:
                    # Add speculative futures to current list.
                    fs.extend(spec_futures)
                    speculative_counter_map += len(spec_futures)
                    phase1_tasks_speculated.extend(index_speculated)
                    # for idx in index_speculated:
                    #     invocation_time_info['phase1_speculative_{}'.format(idx)] = {'start': time.time() - zero_time}
                    # invocation_time_info['phase1_monitor']['end'] = time.time() - zero_time
                    running_tasks += len(index_speculated)

        if speculative_phase2 and phase2_w_started:
            if len(phase2_tasks_speculated) < num_tasks_phase2:
                spec_futures, index_speculated = _check_speculative_futures(internal_storage, phase2_futures_path)
                if len(spec_futures) > 0:
                    # Add speculative futures to current list.
                    phase2_futures.extend(spec_futures)
                    speculative_counter_reduce += len(spec_futures)
                    phase2_tasks_speculated.extend(index_speculated)
                    # for idx in index_speculated:
                    #     invocation_time_info['phase2_speculative_{}'.format(idx)] = {
                    #         'start': time.time() - zero_time}
                    # invocation_time_info['phase2_monitor']['end'] = time.time() - zero_time
                    running_tasks += len(index_speculated)

        _wait_storage(fs,
                      internal_storage,
                      download_results,
                      throw_except,
                      RETURN_EARLY_N,
                      MAX_DIRECT_QUERY_N,
                      pbar=pbar,
                      random_query=RANDOM_QUERY,
                      THREADPOOL_SIZE=threadpool_size)

        if len(phase2_futures) > 0:
            _wait_storage(phase2_futures,
                          internal_storage,
                          download_results,
                          throw_except,
                          RETURN_EARLY_N,
                          MAX_DIRECT_QUERY_N,
                          pbar=pbar,
                          random_query=RANDOM_QUERY,
                          THREADPOOL_SIZE=threadpool_size)

        # Task end control is done from the client using functions' futures.
        # Kill unuseful task attempts and define correct attempt.
        if speculative_phase1:
            for (index, f) in enumerate(fs):
                if f.done:
                    if index is not num_tasks_phase1:
                        # Ended future has not been processed before and is not the monitor function.
                        if index > num_tasks_phase1:
                            # It is an speculative attempt.
                            is_speculative = 1
                            task_index = int(f.call_id) - 1
                        else:
                            # It is an original attempt.
                            is_speculative = 0
                            task_index = index

                        if task_index not in phase1_index_finished:
                            running_tasks -= 1
                            effective_map_ft.append(f)
                            print("Finished phase1 task {} - speculative {}".format(task_index, is_speculative))
                            # if is_speculative:
                                # mitigation_counter_reduce += 2
                                # invocation_time_info['phase1_speculative_{}'.format(task_index)][
                                #     'end'] = time.time() - zero_time
                            # else:
                            #     invocation_time_info['phase1_{}'.format(task_index)]['end'] = time.time() - zero_time
                            invocation_time_info['phase1'][task_index]['end'] = time.time() - zero_time
                            if is_speculative and task_index not in phase1_tasks_speculated:
                                phase1_tasks_speculated.append(task_index)
                            phase1_valid_indexes[str(task_index)] = index
                            phase1_index_finished.append(task_index)
                            phase1_faster_speculatives[str(task_index)] = is_speculative

                            # Kill tasks equivalent to the one finished.
                            if task_index not in phase1_index_killed and task_index in phase1_tasks_speculated:
                                running_tasks -= 1
                                if is_speculative:
                                    fname = str(task_index)
                                else:
                                    fname = "{}s".format(task_index)

                                print("Signaling p1 end on {}/{}.pickle".format(phase2_end_path, fname))
                                mitigation_counter_map +=1
                                _signal_function_end(internal_storage, phase1_end_path, task_index, not is_speculative)
                                # if is_speculative:
                                #     invocation_time_info['phase1_{}'.format(task_index)][
                                #         'end'] = time.time() - zero_time
                                # else:
                                #     invocation_time_info['phase1_speculative_{}'.format(task_index)][
                                #         'end'] = time.time() - zero_time
                                phase1_index_killed.append(task_index)
                    else:
                        if index is num_tasks_phase1 and not phase1_monitor_finished:
                            # monitor task has finished.
                            # invocation_time_info['phase1_monitor']['end'] = time.time() - zero_time
                            phase1_monitor_finished = True
        else:
            for (index, f) in enumerate(fs):
                if f.done and index not in phase1_index_finished:
                    phase1_valid_indexes[str(index)] = index
                    phase1_index_finished.append(index)

        if speculative_phase2 and phase2_w_started:
            for (index, f) in enumerate(phase2_futures):
                if f.done:
                    if index is not num_tasks_phase2:
                        # Ended future has not been processed before and is not the monitor function.
                        if index > num_tasks_phase2:
                            # It is an speculative attempt.
                            is_speculative = 1
                            task_index = int(f.call_id) - 1
                        else:
                            # It is an original attempt.
                            is_speculative = 0
                            task_index = index

                        if task_index not in phase2_index_finished:
                            running_tasks -= 1
                            print("Finished phase2 task {} - speculative {}".format(task_index, is_speculative))
                            if is_speculative and task_index not in phase2_tasks_speculated:
                                phase2_tasks_speculated.append(task_index)
                            phase2_valid_indexes[str(task_index)] = index
                            phase2_index_finished.append(task_index)
                            phase2_faster_speculatives[str(task_index)] = is_speculative
                            # if is_speculative:
                            #     invocation_time_info['phase2_speculative_{}'.format(task_index)][
                            #         'end'] = time.time() - zero_time
                            # else:
                            #     invocation_time_info['phase2_{}'.format(task_index)]['end'] = time.time() - zero_time
                            invocation_time_info['phase2'][task_index]['end'] = time.time() - zero_time
                            # Kill tasks equivalent to the one finished.
                            if task_index not in phase2_index_killed and task_index in phase2_tasks_speculated:
                                running_tasks -= 1
                                if is_speculative:
                                    fname = str(task_index)
                                else:
                                    fname = "{}s".format(task_index)

                                print("Signaling p2 end on {}/{}.pickle".format(phase2_end_path, fname))
                                mitigation_counter_reduce += 1
                                _signal_function_end(internal_storage, phase2_end_path, task_index, not is_speculative)
                                # if is_speculative:
                                #     invocation_time_info['phase2_{}'.format(task_index)][
                                #         'end'] = time.time() - zero_time
                                # else:
                                #     invocation_time_info['phase2_speculative_{}'.format(task_index)][
                                #         'end'] = time.time() - zero_time
                                phase2_index_killed.append(task_index)
                    else:
                        if index is num_tasks_phase2 and not phase2_monitor_finished:
                            # monitor task has finished.
                            print("Phase2 monitor task finished")
                            # invocation_time_info['phase2_monitor']['end'] = time.time() - zero_time
                            phase2_monitor_finished = True
        else:
            for (index, f) in enumerate(phase2_futures):
                if f.done and index not in phase2_index_finished:
                    phase2_valid_indexes[str(index)] = index
                    phase2_index_finished.append(index)

        # Check if asynchronous reducers can be launched.
        if len(
                phase1_index_finished) > num_tasks_phase1 * ASYNCHRONOUS_COMPLETE_PERCENTAGE and not phase2_to_invoke.empty():
            print("will launch reducers")
            invocation_time_info['reducer_invocation_time'] = time.time() - zero_time
            runnable_task_margin = MAX_CONCURRENT_WORKERS - running_tasks - 1
            next_phase2 = [phase2_to_invoke.get() for i in range(min(runnable_task_margin, num_phase2))]
            print("Invoking phase2 {}".format(next_phase2))
            # # We used patched run to call specific function indexes.
            rfs = invoker.run(phase2_job, next_phase2)
            phase2_futures = phase2_futures + rfs
            running_tasks += min(runnable_task_margin, num_phase2)
            if speculative_phase2 and not phase2_w_started:
                rfs_m = invoker.run(phase2_monitor_job, [0])
                phase2_futures = phase2_futures + rfs_m
                running_tasks += 1
            # for i in range(num_tasks_phase1):
            #     invocation_time_info['phase2_{}'.format(i)] = {'start': time.time() - zero_time}
            for i in range(num_tasks_phase1):
                invocation_time_info['phase2'][i] = {'start': time.time() - zero_time}
            # invocation_time_info['phase2_monitor'.format(i)] = {'start': time.time() - zero_time}
            phase2_w_started = True

        if speculative_phase1:
            if len(phase1_index_finished) == num_tasks_phase1:
                # Ensure that there are no speculative futures left.
                spec_futures, new_task_speculated = _check_speculative_futures(internal_storage, phase1_futures_path)
                if len(spec_futures) > 0:
                    fs.extend(spec_futures)
                    N = len(fs)
                    # End possible unfinished speculatives.
                    for task_index in new_task_speculated:
                        running_tasks -= 1
                        _signal_function_end(internal_storage, phase1_end_path, task_index, True)

        if len(phase1_index_finished) > 0:
            time.sleep(wait_dur_sec)
        else:
            time.sleep(WHAIT_WHEN_FINISHED)

    if speculative_phase1:
        if fs[monitor_func_pos].done:
            print("phase1 monitor already finished")
        else:
            _signal_function_end(internal_storage, phase1_end_path, monitor_func_pos, False, is_monitor=True)
    if speculative_phase2:
        if phase2_futures[num_tasks_phase2].done:
            print("phase2 monitor already finished")
        else:
            _signal_function_end(internal_storage, phase2_end_path, num_tasks_phase2, False, is_monitor=True)

    # Delete intermediate temporal files.
    if speculative_phase1:
        temporal_data1 = internal_storage.list_tmp_data(phase1_end_path)
        temporal_data1.extend(internal_storage.list_tmp_data(phase1_communication_path))
        temporal_data1.extend(internal_storage.list_tmp_data(phase1_futures_path))
        temporal_data1.extend(internal_storage.list_tmp_data(phase1_start_path))
        temporal_data1.extend(internal_storage.list_tmp_data("{}_{}".format(BACKUP_INFO_PATH,
                                                                            fs[monitor_func_pos].job_id)))
        internal_storage.delete_temporal_data(temporal_data1)

    if speculative_phase2:
        temporal_data2 = internal_storage.list_tmp_data(phase2_end_path)
        temporal_data2.extend(internal_storage.list_tmp_data(phase2_communication_path))
        temporal_data2.extend(internal_storage.list_tmp_data(phase2_futures_path))
        temporal_data2.extend(internal_storage.list_tmp_data(phase2_start_path))
        temporal_data2.extend(internal_storage.list_tmp_data("{}_{}".format(BACKUP_INFO_PATH,
                                                                            phase2_futures[num_tasks_phase2].job_id)))
        internal_storage.delete_temporal_data(temporal_data2)

    unpatch_invoker_numbered_run(invoker)
    invocation_time_info['num_map_speculatives'] = speculative_counter_map
    invocation_time_info['num_reduce_speculatives'] = speculative_counter_reduce
    invocation_time_info['mitigated_map_stragglers'] = mitigation_counter_map
    invocation_time_info['mitigated_reduce_stragglers'] = mitigation_counter_reduce
    invocation_time_info['sort_time'] = time.time() - zero_time
    # print(invocation_time_info)

    return phase2_futures, phase2_valid_indexes, effective_map_ft, invocation_time_info


def _check_speculative_futures(internal_storage, speculative_path):
    # print("Retrieving speculative futures from {}".format(speculative_path))
    # list contents
    spec_futures = []
    tasks_speculated = []
    spec_futures_names = internal_storage.list_tmp_data(speculative_path)
    if len(spec_futures_names) > 0:
        print("speculative_futures")
        print(spec_futures_names)
        for f in spec_futures_names:
            print("Downloading future {}".format(f))
            # Get task index.
            task_number = int(os.path.splitext(os.path.basename(f))[0])
            tasks_speculated.append(task_number)
            spec_futures.append(pickle.loads(internal_storage.get_data(f)))
        # Remove futures from COS
        internal_storage.delete_temporal_data(spec_futures_names)

    return spec_futures, tasks_speculated


def _signal_function_end(internal_storage, end_path, function_index, is_speculative, is_monitor=False):
    if is_monitor:
        print("Signaling monitor end into {}/monitor.pickle".format(end_path))
        internal_storage.put_data("{}/monitor.pickle".format(end_path), pickle.dumps(function_index))
    else:
        if is_speculative:
            fname = "{}s".format(function_index)
        else:
            fname = str(function_index)
        print("signaling function end to {} - speculative {} ".format(fname, is_speculative))
        internal_storage.put_data("{}/{}.pickle".format(end_path, fname), pickle.dumps(function_index))
