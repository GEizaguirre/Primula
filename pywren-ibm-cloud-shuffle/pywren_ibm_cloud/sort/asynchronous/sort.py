from pywren_ibm_cloud.sort.asynchronous.patch import patch_map_asynchronous, unpatch_map_asynchronous
from pywren_ibm_cloud.sort.asynchronous.phase2_standalone_no_pipeline import _reduce_partitions as _reduce_partitions_standalone
from pywren_ibm_cloud.sort.asynchronous.phase2_monitored_no_pipeline import _reduce_partitions as _reduce_partitions_monitored
from pywren_ibm_cloud.compute.backends.ibm_cf.config import MAX_CONCURRENT_WORKERS
from pywren_ibm_cloud.sort.phase1 import _create_mapper_args
from pywren_ibm_cloud.sort.phase1_monitored import _partition_into_segments_monitored
# from pywren_ibm_cloud.sort.tests.phase1_alternatives.phase1_sort_c_sort import _partition_into_segments_monitored, _create_mapped_args
from pywren_ibm_cloud.sort.phase1_standalone import _partition_into_segments_standalone
from pywren_ibm_cloud.sort.phase2 import _create_reducer_args


def sort_async(pw, num_workers_phase1, num_workers_phase2, parser_info_path, parser_info_bucket,
               speculative_map=False, speculative_reduce=False):


    segm_groups = list()
    for i in range(min(num_workers_phase2, MAX_CONCURRENT_WORKERS)):
        segm_groups.append({'segms': list()})
    for i in range(num_workers_phase2):
        segm_groups[i % min(num_workers_phase2, MAX_CONCURRENT_WORKERS)]['segms'].append(i)


    if speculative_map:
        _partition_into_segments = _partition_into_segments_monitored
    else:
        _partition_into_segments = _partition_into_segments_standalone

    if speculative_reduce:
        _reduce_partitions = _reduce_partitions_monitored
    else:
        _reduce_partitions = _reduce_partitions_standalone

    ft, reduce_job, reduce_monitor_job = pw.map_reduce(_partition_into_segments, map_args,
                                                       _reduce_partitions,
                                                       reduce_args,
                                                       speculative_map=speculative_map,
                                                       speculative_reduce=speculative_reduce)

    res = pw.get_result(reduce_job, fs=ft, speculative_map=speculative_map, speculative_reduce=speculative_reduce,
                                            reduce_monitor_job=reduce_monitor_job)
