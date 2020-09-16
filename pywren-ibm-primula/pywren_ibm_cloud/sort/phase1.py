from pywren_ibm_cloud.sort.monitor.patch import patch_map_monitor, unpatch_map_monitor
# Hadoop-like phase1
# from pywren_ibm_cloud.sort.tests.phase1_alternatives.phase1_hadoop import _partition_into_segments_standalone_hadoop
# sort-c-sort
from pywren_ibm_cloud.sort.phase1_monitored import _partition_into_segments_monitored
from pywren_ibm_cloud.sort.phase1_standalone import _partition_into_segments_standalone
from pywren_ibm_cloud.sort.tests.phase1_alternatives.phase1_sort_c_sort import _partition_into_segments_standalone as _partition_into_segments_standalone_scs
# Basic sort comparisons
# from pywren_ibm_cloud.sort.tests.basic_sort import _partition_into_segments_basic
# Granulated vs ungranulated read throughput comparison
# from pywren_ibm_cloud.sort.tests.granulated_performance.ungranulated_read import _partition_into_segments_basic as _read_ungranulated
# from pywren_ibm_cloud.sort.tests.granulated_performance.granulated_read import _partition_into_segments_basic as _read_granulated
# Concurrent vs sequential read throughput comparison
# from pywren_ibm_cloud.sort.tests.granulated_performance.granulated_read_concurrent import _partition_into_segments_basic as _read_concurrent
# from pywren_ibm_cloud.sort.tests.granulated_performance.granulated_read import _partition_into_segments_basic as _read_sequential


def partitions_into_segments_csv(pw, num_workers, parser_info_path, parser_info_bucket, speculative=False):
    print("Map with parallel extraction")

    # Generate arguments with metadata.
    arguments = _create_mapper_args(int(num_workers),
                                    parser_info_path,
                                    parser_info_bucket)

    # Patch correct map function (speculative/not speculative)
    if speculative:
        patch_map_monitor(pw)
        _partition_into_segments = _partition_into_segments_monitored
    else:
        _partition_into_segments = _partition_into_segments_standalone

    ft = pw.map(_partition_into_segments, arguments)
    res = pw.get_result(ft)

    # print(res)

    if speculative:
        unpatch_map_monitor(pw)


def _create_mapper_args(num_maps, parser_info_path, parser_info_bucket):
    arguments = [
        {'args': {
            'index': i,
            'parser_info_path': parser_info_path,
            'parser_info_bucket': parser_info_bucket
        }}
        for i in range(num_maps)
    ]
    return arguments


########################################################################################################################