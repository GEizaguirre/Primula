#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import sys
import time
import pickle
import logging
import traceback
from six import reraise
from pywren_ibm_cloud.storage import InternalStorage
from pywren_ibm_cloud.storage.utils import check_storage_path, get_storage_path
from pywren_ibm_cloud.libs.tblib import pickling_support

pickling_support.install()
logger = logging.getLogger(__name__)


class ResponseFuture:
    """
    Object representing the result of a PyWren invocation. Returns the status of the
    execution and the result when available.
    """
    class State():
        New = "New"
        Invoked = "Invoked"
        Running = "Running"
        Ready = "Ready"
        Success = "Success"
        Futures = "Futures"
        Error = "Error"

    GET_RESULT_SLEEP_SECS = 1
    GET_RESULT_MAX_RETRIES = 10

    def __init__(self, executor_id, job_id, call_id, storage_config, execution_timeout, call_metadata):
        self.log_level = os.getenv('PYWREN_LOGLEVEL')
        self.call_id = call_id
        self.job_id = job_id
        self.executor_id = executor_id
        self.storage_config = storage_config
        self.execution_timeout = execution_timeout

        self.produce_output = True
        self.read = False

        self._state = ResponseFuture.State.New
        self._exception = Exception()
        self._handler_exception = None
        self._return_val = None
        self._new_futures = None
        self._traceback = None
        self._call_status = None
        self._call_output = None
        self._call_metadata = call_metadata.copy()

        self.activation_id = self._call_metadata.pop('activation_id', None)

        self.status_query_count = 0
        self.output_query_count = 0

        self.storage_path = get_storage_path(self.storage_config)

    def _set_state(self, new_state):
        self._state = new_state

    def cancel(self):
        raise NotImplementedError("Cannot cancel dispatched jobs")

    def cancelled(self):
        raise NotImplementedError("Cannot cancel dispatched jobs")

    @property
    def running(self):
        return self._state == ResponseFuture.State.Running

    @property
    def error(self):
        return self._state == ResponseFuture.State.Error

    @property
    def futures(self):
        """
        The response of a call was a FutureResponse instance.
        It has to wait to the new invocation output.
        """
        return self._state == ResponseFuture.State.Futures

    @property
    def done(self):
        if self._state in [ResponseFuture.State.Success, ResponseFuture.State.Futures, ResponseFuture.State.Error]:
            return True
        return False

    @property
    def ready(self):
        if self._state in [ResponseFuture.State.Ready, ResponseFuture.State.Futures, ResponseFuture.State.Error]:
            return True
        return False

    def status(self, throw_except=True, internal_storage=None):
        """
        Return the status returned by the call.
        If the call raised an exception, this method will raise the same exception
        If the future is cancelled before completing then CancelledError will be raised.

        :param check_only: Return None immediately if job is not complete. Default False.
        :param throw_except: Reraise exception if call raised. Default true.
        :param storage_handler: Storage handler to poll cloud storage. Default None.
        :return: Result of the call.
        :raises CancelledError: If the job is cancelled before completed.
        :raises TimeoutError: If job is not complete after `timeout` seconds.
        """
        if self._state == ResponseFuture.State.New:
            raise ValueError("task not yet invoked")

        if self._state in [ResponseFuture.State.Ready, ResponseFuture.State.Success]:
            return self._call_status

        if internal_storage is None:
            internal_storage = InternalStorage(self.storage_config)

        if self._call_status is None:
            check_storage_path(internal_storage.get_storage_config(), self.storage_path)
            self._call_status = internal_storage.get_call_status(self.executor_id, self.job_id, self.call_id)
            self.status_query_count += 1

            while self._call_status is None:
                time.sleep(self.GET_RESULT_SLEEP_SECS)
                self._call_status = internal_storage.get_call_status(self.executor_id, self.job_id, self.call_id)
                self.status_query_count += 1

        self.activation_id = self._call_status.get('activation_id', None)

        if self._call_status['type'] == '__init__':
            self._set_state(ResponseFuture.State.Running)
            return self._call_status

        if self._call_status['exception']:
            self._set_state(ResponseFuture.State.Error)
            self._exception = pickle.loads(eval(self._call_status['exc_info']))

            if not self._call_status.get('exc_pickle_fail', False):
                fn_exctype = self._exception[0]
                fn_exc = self._exception[1]

                if self._handler_exception is None:
                    if fn_exc.args and fn_exc.args[0] == "HANDLER":
                        self._handler_exception = True
                        del fn_exc.errno
                        fn_exc.args = (fn_exc.args[1],)
                    else:
                        self._handler_exception = False

                msg1 = ('ExecutorID {} | JobID {} - There was an exception - Activation '
                        'ID: {}'.format(self.executor_id, self.job_id, self.activation_id))

                def exception_hook(exctype, exc, trcbck):
                    if exctype == fn_exctype and exc == fn_exc:
                        msg2 = '--> Exception: {} - {}'.format(fn_exctype.__name__, fn_exc)
                        print(msg1) if not self.log_level else logger.info(msg1)
                        if self._handler_exception:
                            print(msg2+'\n') if not self.log_level else logger.info(msg2)
                        else:
                            traceback.print_exception(*self._exception)
                    else:
                        sys.excepthook = sys.__excepthook__
                        traceback.print_exception(exctype, exc, trcbck)
            else:
                fn_exc = Exception(self._exception['exc_value'])
                self._exception = (Exception, fn_exc, self._exception['exc_traceback'])

            if throw_except:
                sys.excepthook = exception_hook
                reraise(*self._exception)
            else:
                logger.info(msg1)
                logger.debug('Exception: {} - {}'.format(self._exception[0].__name__, self._exception[1]))
                return None

        self._call_metadata['host_submit_time'] = self._call_status['host_submit_time']
        self._call_metadata['status_done_timestamp'] = time.time()
        self._call_metadata['status_query_count'] = self.status_query_count

        total_time = format(round(self._call_status['end_time'] - self._call_status['start_time'], 2), '.2f')
        log_msg = ('ExecutorID {} | JobID {} - Got status from call {} - Activation '
                   'ID: {} - Time: {} seconds'.format(self.executor_id,
                                                      self.job_id,
                                                      self.call_id,
                                                      self.activation_id,
                                                      str(total_time)))
        logger.info(log_msg)
        self._set_state(ResponseFuture.State.Ready)

        if not self._call_status['result']:
            self.produce_output = False

        if not self.produce_output:
            self._set_state(ResponseFuture.State.Success)

        if 'new_futures' in self._call_status:
            self.result(throw_except=throw_except, internal_storage=internal_storage)

        return self._call_status

    def result(self, throw_except=True, internal_storage=None):
        """
        Return the value returned by the call.
        If the call raised an exception, this method will raise the same exception
        If the future is cancelled before completing then CancelledError will be raised.

        :param throw_except: Reraise exception if call raised. Default true.
        :param internal_storage: Storage handler to poll cloud storage. Default None.
        :return: Result of the call.
        :raises CancelledError: If the job is cancelled before completed.
        :raises TimeoutError: If job is not complete after `timeout` seconds.
        """
        if self._state == ResponseFuture.State.New:
            raise ValueError("task not yet invoked")

        if self._state == ResponseFuture.State.Success:
            return self._return_val

        if self._state == ResponseFuture.State.Futures:
            return self._new_futures

        if internal_storage is None:
            internal_storage = InternalStorage(storage_config=self.storage_config)

        self.status(throw_except=throw_except, internal_storage=internal_storage)

        if self._state == ResponseFuture.State.Success:
            return self._return_val

        if self._state == ResponseFuture.State.Futures:
            return self._new_futures

        call_output_time = time.time()
        call_output = internal_storage.get_call_output(self.executor_id, self.job_id, self.call_id)
        self.output_query_count += 1

        while call_output is None and self.output_query_count < self.GET_RESULT_MAX_RETRIES:
            time.sleep(self.GET_RESULT_SLEEP_SECS)
            call_output = internal_storage.get_call_output(self.executor_id, self.job_id, self.call_id)
            self.output_query_count += 1

        if call_output is None:
            if throw_except:
                raise Exception('Unable to get the output from call {} - '
                                'Activation ID: {}'.format(self.call_id, self.activation_id))
            else:
                self._set_state(ResponseFuture.State.Error)
                return None

        call_output = pickle.loads(call_output)
        call_output_time_done = time.time()
        self._call_output = call_output

        self._call_metadata['download_output_time'] = call_output_time_done - call_output_time
        self._call_metadata['output_query_count'] = self.output_query_count
        self._call_metadata['download_output_timestamp'] = call_output_time_done

        log_msg = ('ExecutorID {} | JobID {} - Got output from call {} - Activation '
                   'ID: {}'.format(self.executor_id, self.job_id, self.call_id, self.activation_id))
        logger.info(log_msg)

        function_result = call_output['result']

        if isinstance(function_result, ResponseFuture) or \
           (type(function_result) == list and len(function_result) > 0 and isinstance(function_result[0], ResponseFuture)):
            self._new_futures = [function_result] if type(function_result) == ResponseFuture else function_result
            self._set_state(ResponseFuture.State.Futures)
            self._call_metadata['status_done_timestamp'] = self._call_metadata['download_output_timestamp']
            del self._call_metadata['download_output_timestamp']
            return self._new_futures

        else:
            self._return_val = function_result
            self._set_state(ResponseFuture.State.Success)
            return self._return_val
