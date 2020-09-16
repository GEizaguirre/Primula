import numpy as np


def ds_dims(coordinates):
    min_x, min_y, aux = np.amin(coordinates, axis=0)
    max_x, max_y, aux = np.amax(coordinates, axis=0)
    nrows, ncols = max_y - min_y + 1, max_x - min_x + 1
    return nrows, ncols


def get_pixel_indices(coordinates):
    _coord = np.array(coordinates)
    _coord = np.around(_coord, 5)
    _coord -= np.amin(_coord, axis=0)
    _, ncols = ds_dims(coordinates)
    pixel_indices = _coord[:, 1] * ncols + _coord[:, 0]
    pixel_indices = pixel_indices.astype(np.int32)
    return pixel_indices


def fragment_dataset_into_chunks_imzml(num_workers, sp_n, cnk_size=None):
    print("Dataset fragmentation started")
    # Get chunk size from ibm cos
    if cnk_size is not None:
        # chunk_size_file ="tmp/chunk_size.msgpack"
        # chunk_size = msgpack.loads(ibm_cos.get_object(Bucket=bucket, Key=chunk_size_file)['Body'].read())
        chunk_size = cnk_size
    else:
        chunk_size = (sp_n // num_workers) + 1
    print("Got chunk size {}".format(chunk_size))

    sp_i_bounds = []
    sp_i_bound = 0
    while sp_i_bound <= sp_n:
        sp_i_bounds.append(sp_i_bound)
        sp_i_bound += chunk_size
    sp_i_bounds.append(sp_n)

    print("Bounds")
    for bnd in sp_i_bounds:
        print(bnd)

    return sp_i_bounds


def fragment_dataset_into_chunks_csv(num_workers, total_size, cnk_size=None):
    print("Dataset fragmentation started")
    # Get chunk size from ibm cos
    if cnk_size is not None:
        # chunk_size_file ="tmp/chunk_size.msgpack"
        # chunk_size = msgpack.loads(ibm_cos.get_object(Bucket=bucket, Key=chunk_size_file)['Body'].read())
        chunk_size = cnk_size
    else:
        chunk_size = (total_size // num_workers) + 1
    print("Got chunk size {}".format(chunk_size))

    sp_i_bounds = []
    sp_i_bound = 0
    while sp_i_bound <= total_size:
        sp_i_bounds.append(sp_i_bound)
        sp_i_bound += chunk_size
    sp_i_bounds.append(total_size)

    return sp_i_bounds
