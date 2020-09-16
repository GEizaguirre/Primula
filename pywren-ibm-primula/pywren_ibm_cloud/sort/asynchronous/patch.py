from pywren_ibm_cloud import invoker
from pywren_ibm_cloud import executor


def patch_invoker_numbered_run(inv):
    from pywren_ibm_cloud.sort.asynchronous.invoker import run
    inv.run = run.__get__(inv, invoker.FunctionInvoker)


def unpatch_invoker_numbered_run(inv):
    inv.run = invoker.FunctionInvoker.run.__get__(inv, invoker.FunctionInvoker)


def patch_map_asynchronous(pw):
    from pywren_ibm_cloud.sort.asynchronous.executor import map_reduce, wait, get_result

    pw.map_reduce = map_reduce.__get__(pw, executor.FunctionExecutor)
    pw.wait = wait.__get__(pw, executor.FunctionExecutor)
    pw.get_result = get_result.__get__(pw, executor.FunctionExecutor)


def unpatch_map_asynchronous(pw):
    pw.map_reduce = executor.FunctionExecutor.map_reduce.__get__(pw, executor.FunctionExecutor)
    pw.wait = executor.FunctionExecutor.wait.__get__(pw, executor.FunctionExecutor)
    pw.get_result = executor.FunctionExecutor.get_result.__get__(pw, executor.FunctionExecutor)
