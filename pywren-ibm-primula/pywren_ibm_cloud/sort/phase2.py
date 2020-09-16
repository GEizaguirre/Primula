from pywren_ibm_cloud.compute.backends.ibm_cf.config import MAX_CONCURRENT_WORKERS
from pywren_ibm_cloud.sort.monitor.patch import patch_map_monitor, unpatch_map_monitor
from pywren_ibm_cloud.sort.config import PAR_LEVEL_LIMIT_PER_FUNCTION
from pywren_ibm_cloud.sort.phase2_monitored import _reduce_partitions_monitored
from pywren_ibm_cloud.sort.phase2_standalone import _reduce_partitions_standalone
from pywren_ibm_cloud.sort.tests.basic_sort import _reduce_partitions_basic


def reduce_partition_segments(pw, num_segments, parser_info_path, parser_info_bucket, speculative=False):

    segm_groups = list()
    for i in range(min(num_segments, MAX_CONCURRENT_WORKERS)):
        segm_groups.append({'segms': list()})
    for i in range(num_segments):
        segm_groups[i % min(num_segments, MAX_CONCURRENT_WORKERS)]['segms'].append(i)

    print(segm_groups)

    if speculative:
        _reduce_partitions = _reduce_partitions_monitored
    else:
        _reduce_partitions = _reduce_partitions_standalone

    # TODO: remove
    # _reduce_partitions = _reduce_partitions_basic

    arguments = _create_reducer_args(segm_groups, parser_info_bucket, parser_info_path)

    futures = pw.map(_reduce_partitions, arguments)

    res = pw.get_result(futures)

    print(res)
    print(sum(res))


def _create_reducer_args(segm_groups, parser_info_bucket, parser_info_path):
    arguments = [
        { 'args' : {
            'index' : idx,
            'segms' : segms['segms'],
            'parser_info_path': parser_info_path,
            'parser_info_bucket': parser_info_bucket
        }}
        for idx, segms in enumerate(segm_groups)
    ]
    #print(arguments)
    return arguments


