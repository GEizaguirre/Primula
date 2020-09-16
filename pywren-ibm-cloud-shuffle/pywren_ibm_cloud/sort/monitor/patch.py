from pywren_ibm_cloud import executor


def patch_map_monitor(pw):
    from pywren_ibm_cloud.sort.monitor.executor import map, wait, get_result

    pw.map = map.__get__(pw, executor.FunctionExecutor)
    pw.wait = wait.__get__(pw, executor.FunctionExecutor)
    pw.get_result = get_result.__get__(pw, executor.FunctionExecutor)


def unpatch_map_monitor(pw):
    pw.map = executor.FunctionExecutor.map.__get__(pw, executor.FunctionExecutor)
    pw.wait = executor.FunctionExecutor.wait.__get__(pw, executor.FunctionExecutor)
    pw.get_result = executor.FunctionExecutor.get_result.__get__(pw, executor.FunctionExecutor)
