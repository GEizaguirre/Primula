import math

from pywren_ibm_cloud.sort.monitor.config import DEFAULT_CI_FACTOR, SPECULATIVE_SLOWTASK_THRESHOLD, \
    LAST_TASK_PROPORTION_THRESHOLD_RUNTIME


class DataStatistics:
    count = 0
    sum = 0
    sumSquares = 0

    def _init(self, init_num):
        self.count = 1
        self.sum = init_num
        self.sum_squares = init_num * init_num

    def add(self, new_num):
        self.count += 1
        self.sum += new_num
        self.sum_squares += new_num * new_num

    def remove(self, old_num):
        self.count -= 1
        self.sum -= old_num
        self.sum_squares -= old_num * old_num

    def update_statistics(self, old, update):
        self.sum += update - old
        self.sum_squares += (update * update) - (old * old)

    def mean(self):
        if self.count == 0:
            return 0.0
        else:
            self.sum / self.count

    def var(self):
        # E(X^2) - E(X)^2
        if self.count <= 1:
            return 0.0

        mean = self.mean()
        return max((self.sum_squares / self.count) - mean * mean, 0.0)

    def std(self):
        return math.sqrt(self.var())

    def outlier(self, sigma):
        if self.count != 0.0:
            return self.mean() + self.std() * sigma
        return 0.0

    # * calculates the mean value within 95% ConfidenceInterval.
    # * 1.96 is standard for 95 %
    def meanCI(self):
        if self.count <= 1:
            return 0.0

        currMean = self.mean()
        currStd = self.std()
        return currMean + (DEFAULT_CI_FACTOR * currStd / math.sqrt(self.count));


# class DefaultIBMEstimator:
#
#     def _init_(self, task_ids):
#         self.statistics = DataStatistics()
#         self.done_tasks = []
#         self.proc_tasks = []
#         self.start_times = {}
#         self.total_times = {}
#         self.total_tasks = task_ids
#         self.slow_task_relative_threshold = SPECULATIVE_SLOWTASK_THRESHOLD
#         print("Default estimator started")
#
#     def update_stats(self, task):
#         if task['id'] not in self.proc_tasks:
#             self.statistics.add(task['total_time'])
#         else:
#             self.statistics.remove(self.total_times[task['id']])
#             self.statistics.add(task['total_time'])
#
#     def new_done(self, task):
#         self.total_times[task['id']] = task['total_time']
#         if task['id'] not in self.done_tasks:
#             self.done_tasks.append(task['id'])
#         if task['id'] not in self.proc_tasks:
#             self.statistics.add(task['total_time'])
#         else:
#             self.statistics.remove(self.total_times[task['id']])
#             self.statistics.add(task['total_time'])
#
#     def threshold_runtime(self, task):
#         # TODO update done tasks from outside
#         completed_tasks = len(self.done_tasks)
#         registered_tasks = len(self.total_times)
#         num_tasks = len(self.total_tasks)
#         # if completed_tasks < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE or completed_tasks/total_tasks < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE:
#         if registered_tasks/num_tasks < MINIMUM_REGISTERED_PROPORTION_TO_SPECULATE:
#             return RUNTIME_MAX_VALUE
#         else:
#             return self.statistics.outlier(self.slow_task_relative_threshold)
#
#     def estimated_runtime(self, attempt):
#         if (attempt['progress'] == 1):
#             return attempt['total_time']
#         else:
#             return (1 - attempt['progress']) * (attempt['progress'] / attempt['total_time'])


class DefaultIBMEstimator:

    def __init__(self, task_indexes):
        self.task_indexes = task_indexes
        print("Default estimator started")

    def threshold_runtime(self, task_runtime):
        print("calculating threshold runtime from {}".format(task_runtime))
        thr = task_runtime * LAST_TASK_PROPORTION_THRESHOLD_RUNTIME
        return max(thr, SPECULATIVE_SLOWTASK_THRESHOLD)

    def estimated_runtime(self, task_info):
        if task_info['progress'] is 1:
            finished = True
            runtime = task_info['time']
        else:
            finished = False
            PS_i = task_info['progress']
            T_i = task_info['time']
            runtime = (1 - float(PS_i)) / (float(PS_i) / T_i)

        return runtime, finished


