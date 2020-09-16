from numpy import amin, amax, around, int32
from numpy.core._multiarray_umath import array


def ds_dims(coordinates):
    min_x, min_y, aux = amin(coordinates, axis=0)
    max_x, max_y, aux = amax(coordinates, axis=0)
    nrows, ncols = max_y - min_y + 1, max_x - min_x + 1
    return nrows, ncols


def get_pixel_indices(coordinates):
    _coord = array(coordinates)
    _coord = around(_coord, 5)
    _coord -= amin(_coord, axis=0)
    _, ncols = ds_dims(coordinates)
    pixel_indices = _coord[:, 1] * ncols + _coord[:, 0]
    pixel_indices = pixel_indices.astype(int32)
    return pixel_indices