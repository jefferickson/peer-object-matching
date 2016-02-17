from libc.math cimport sqrt

import ObjectMatch.utils as utils


cpdef float euclid_distance(coords1, coords2):
    '''Calculates the Euclidean distance between two points of equal dimension.'''

    cdef int dimensions = len(coords1)

    if len(coords2) != dimensions:
        raise utils.DiffNumOfDims('ERROR: the objects are not the same num of dims!')

    cdef float sum = 0
    cdef int i = 0
    cdef float x
    cdef float y
    for i in range(dimensions):
        x = coords1[i]
        y = coords2[i]
        sum += (x - y) ** 2

    return sqrt(sum)
