from pywren_ibm_cloud.sort.monitor.config import MAX_WAITING_TIME_HEARTBEAT


class TaskAttemptHistoryStatistics:

    def _init_(self, estimated_run_time, progress, start_time):
        self.estimated_run_time = estimated_run_time
        self.progress = progress
        self.last_heart_beat_time = start_time

    def not_heartbeated_in_a_while(self, now):
        if (now - self.last_heart_beat_time <= MAX_WAITING_TIME_HEARTBEAT):
            return False
        else:
            self.last_heart_beat_time = now
            return True

    def reset_heart_beat_time(self, last_heart_beat_time):
        self.last_heart_beat_time = last_heart_beat_time

    def update (self, estimated_run_time, progress, start_time):
        self.estimated_run_time = estimated_run_time
        self.progress = progress
        self.last_heart_beat_time = start_time
