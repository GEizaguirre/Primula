from pywren_ibm_cloud.sort.monitor.config import ATTEMPT_FINISHED


class DefaultIBMProgressCalculator:

    def _init_(self):
        print("Started progress calculator")

    def get_progress_score(attempt_data):
        if attempt_data['state'] == ATTEMPT_FINISHED:
            progress_score = 1
        else:
            # For map tasks, the progress score is given by the
            # fraction of input data read by that time. A reduce
            # task’s execution is divided into three phases, and
            # progressscore is defined as fraction of phases
            # completed (like 1 / 3 or 2 / 3). If the average progress
            # score of a task is less  than the average for its category
            # by more than 0.2, , and it has been running forat least one
            # minute, then it is marked as a straggler.
            progress_score = attempt_data['fraction_read']
        return progress_score