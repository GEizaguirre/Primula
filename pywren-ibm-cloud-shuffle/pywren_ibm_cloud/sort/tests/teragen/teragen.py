from pywren_ibm_cloud import ibm_cf_executor as executor
from pywren_ibm_cloud.sort.tests.teragen.sort_gen_mapper import map_split, complete_upload
from pywren_ibm_cloud.sort.tests.teragen.config import TERAGEN_FUNCTION_RUNTIME_MEMORY, TERAGEN_ROW_SIZE_BYTES, \
    TERAGEN_FUNCTION_RUNTIME_MEMORY_PERCENTAGE
import ibm_boto3
from pywren_ibm_cloud.sort.tests.teragen.input_split import input_split


def teragen(config, num_rows, output_bucket, output_key, num_mappers=None, start_row=0):
    pywren_executor = executor(config=config, runtime_memory=TERAGEN_FUNCTION_RUNTIME_MEMORY)
    ibm_cos = ibm_boto3.client(service_name='s3',
                               aws_access_key_id=config['ibm_cos']['access_key'],
                               aws_secret_access_key=config['ibm_cos']['secret_key'],
                               endpoint_url=config['ibm_cos']['endpoint'])
    total_size = num_rows * TERAGEN_ROW_SIZE_BYTES
    if num_mappers is None:
        num_splits = int(total_size / (TERAGEN_FUNCTION_RUNTIME_MEMORY*(1024**2)*TERAGEN_FUNCTION_RUNTIME_MEMORY_PERCENTAGE) + 1)
    else:
        num_splits = num_mappers
    print("Generating terasort input of size {} B ({} MB, {} GB)".format(total_size,
                                                                         total_size / (1024**2),
                                                                         total_size / (1024**3)))
    print("Using {} mappers".format(num_splits))

    args = { 'args': {
        'num_rows' : num_rows,
        'start_row' : start_row,
        'num_splits' : num_splits,
        'output_bucket' : output_bucket,
        'output_key' : output_key
    }}
    ft = pywren_executor.call_async(input_split, args)
    pywren_executor.wait(ft)
    print("Input split completed, mapping splits")
    fts = pywren_executor.map_reduce(map_split, range(num_splits), complete_upload)
    # def sum(ibm_cos, x):
    #     return x + 1
    #
    # fts = pywren_executor.map(sum, [1, 2, 3, 4])
    pywren_executor.wait(fts)
    print("Teragen completed")
    # pywren_executor.get_result(fts)
