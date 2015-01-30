#! /usr/bin/env python3

##########################################
# Author:   Jeff Erickson <jeff@erick.so>
# Date:     2014-12-28
##########################################

# USAGE: ./obj.match.multicore.py INPUT_FILE OUTPUT_FILE
# INPUT FILE FORMAT: object_id, categorical_group, no_match_group, continuous_data_1, continuous_data_2, ..., continuous_data_n
# OUTPUT FILE FORMAT: object_id, peer_object_id_1, peer_object_id_2, ..., peer_object_id_n

import sys
import csv
import math
import heapq
from datetime import datetime
from concurrent import futures


def euclid_distance(coords1, coords2):
    '''Calculates the Euclidean distance between two points of equal dimension.'''

    if len(coords1) != len(coords2):
        raise DiffNumOfDims('ERROR: the tuples are not the same length!')
    
    sum = 0
    for x in range(len(coords1)):
        sum += (coords1[x] - coords2[x]) ** 2

    return math.sqrt(sum)

def write_peer_groups(f, peer_groups, delimiter = ','):
    '''Writes the object_id and the object_ids of the peers to file.'''

    for object_id, peer_ids in sorted(peer_groups.items()):
        f.write(object_id)
        for peer_id in sorted(peer_ids):
            f.write(delimiter + peer_id)
        f.write('\n')

def calc_a_peer_group(subset_and_whole_group_tuple, max_peer_group_n = 100, min_peer_group_n = None, max_distance_allowed = None):
    '''For a given dict of objects, calculates peer groups for each object compared to the other objects.'''

    # Break up the tuple we got from the generator generate_groups().
    group_subset, whole_group = subset_and_whole_group_tuple
    
    # Iterate over each object in subset, compare it to all other objects, and then find the closest peers.
    peer_groups = {}

    print("{}: Starting process with {} objects.".format(datetime.now(), len(group_subset)))

    for an_object_to_peer in group_subset:
        object_id, object_no_match_group, object_coords = an_object_to_peer
        distances = []

        for a_peer in whole_group:
            peer_object_id, peer_object_no_match_group, peer_object_coords = a_peer
            if object_id == peer_object_id: continue
            # If a no_match_group is defined, make sure it doesn't match.
            if object_no_match_group and object_no_match_group == peer_object_no_match_group: continue
            try:
                distance_between_objects = euclid_distance(object_coords, peer_object_coords)
            except (TypeError, DiffNumOfDims) as e:
                print('Either {} or {} has invalid coordinates.'.format(object_id, peer_object_id))
            if not max_distance_allowed or distance_between_objects <= max_distance_allowed:
                distances.append((peer_object_id, distance_between_objects))
        
        # Let's find the closest objects using a heap.
        peer_group = heapq.nsmallest(max_peer_group_n, distances, key = lambda s: s[1])
        if min_peer_group_n and len(peer_group) < min_peer_group_n:
            raise PeerGroupTooSmall('{} has too few peers.'.format(object_id))
        peer_ids = {peer_object[0] for peer_object in peer_group}

        # Finally add them to our dictionary of results for this subset.
        peer_groups.update({object_id: peer_ids})

    print("{}: Ending process with {} objects.".format(datetime.now(), len(group_subset)))

    # Return the peer group dict to be written to file.
    return peer_groups

def load_calc_output_all_peer_groups(input_file, output_file, delimiter = ',', max_workers = None):
    '''Load object, groups, and coords from file, run peer group calc per group, and output.'''

    # First we will load in all of the data, storing it by the categorical groups because they must be exact matches on that data.
    groups_dict = {}
    with open(input_file) as f: 
        reader = csv.reader(f, delimiter = delimiter)
        for row in reader:
            try:
                object_id, group, no_match_group, *coords = row
            except ValueError as e:
                print('Not enough values in row: {}'.format(row))
            
            coords_tuple = tuple([float(x) for x in coords])
            groups_dict.setdefault(group, []).append((object_id, no_match_group, coords_tuple))
    # Once everything is grouped by categorical identifiers, we can just pull out each group into a list 
    # (we don't need the categorical data anymore as it is already grouped.)
    groups_list = list(groups_dict.values())

    # We are going to utilize the Map-Reduce method. We're going break up each group into pieces, assigning
    # each to a processor thread. Upon return of the calculations, we will write out the results to file.
    with open(output_file, 'w') as f:
        with futures.ProcessPoolExecutor(max_workers = max_workers) as pool:
            for peer_groups in pool.map(calc_a_peer_group, generate_groups(groups_list)):
                write_peer_groups(f, peer_groups)

def generate_groups(groups, max_group_size = 1000):
    '''A generator that slices up each group into chunks to aid with CPU utilization. Yields a tuple of the subset and the whole group.'''
    
    for group in groups:
        for i in range(0, len(group), max_group_size):
            # Yield a tuple of the m x n group to process. m rows with all n peer columns.
            yield group[i:i+max_group_size], group


class PeerGroupTooSmall(Exception):
    '''Exception for when a peer group is below the minimum required.'''
    pass

class DiffNumOfDims(Exception):
    '''Exception for when two tuples have a different number of dimensions.'''
    pass


if __name__ == '__main__':

    #Keep track of the runtime.
    start_time = datetime.now()

    #Input and output files.
    try:
        input_file, output_file = sys.argv[1:]
    except ValueError as e:
        sys.exit('USAGE: ./obj.match.multicore.py INPUT_FILE OUTPUT_FILE')

    #Do the loading, calculations, and output.
    load_calc_output_all_peer_groups(input_file, output_file)

    #How long did it take?
    print(datetime.now() - start_time)
