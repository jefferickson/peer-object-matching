#! /usr/bin/env python3

##########################################
# Author:   Jeff Erickson <jeff@erick.so>
# Date:     2014-12-28
##########################################

# USAGE: ./obj.match.multicore.py INPUT_FILE OUTPUT_FILE

import sys
import csv
import math
import heapq
from datetime import datetime
from concurrent import futures


def euclid_distance(coords1, coords2):
    '''Calculates the Euclidean distance between two points of equal dimension.'''

    assert len(coords1) == len(coords2), "ERROR: the tuples are not the same length!"
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

def calc_a_peer_group(objects, max_peer_group_n = 100, min_peer_group_n = None, max_distance_allowed = None):
    '''For a given dict of objects, calculates peer groups for each object compared to the other objects.'''
    
    # Iterate over each object, compare it to all other objects, and then find the closest peers.
    peer_groups = {}
    for object_id, object_params in objects.items():
        object_no_match_group, coords = object_params
        distances = []

        for peer_object_id, peer_object_params in objects.items():
            peer_object_no_match_group, peer_object_coords = peer_object_params
            if object_id == peer_object_id: continue
            # If a no_match_group is defined, make sure it doesn't match.
            if object_no_match_group and object_no_match_group == peer_object_no_match_group: continue
            try:
                distance_between_objects = euclid_distance(coords, peer_object_coords)
            except TypeError as e:
                print('Either {} or {} has invalid coordinates.'.format(object_id, peer_object_id))
            if not max_distance_allowed or distance_between_objects <= max_distance_allowed:
                distances.append({'peer_object_id': peer_object_id, 'distance': distance_between_objects})
        
        peer_group = heapq.nsmallest(max_peer_group_n, distances, key = lambda s: s['distance'])
        if min_peer_group_n and len(peer_group) < min_peer_group_n:
            raise PeerGroupTooSmall('{} has too few peers.'.format(object_id))
        peer_ids = [peer_object['peer_object_id'] for peer_object in peer_group]
        peer_groups.update({object_id: peer_ids})

    return peer_groups

def load_calc_output_all_peer_groups(input_file, output_file, delimiter = ','):
    '''Load object, groups, and coords from file, run peer group calc per group, and output.'''

    groups_dict = {}
    with open(input_file) as csvfile: 
        reader = csv.reader(csvfile, delimiter = delimiter)
        for row in reader:
            object_id, group, no_match_group, *coords = row
            coords_tuple = tuple([float(x) for x in coords])
            groups_dict.setdefault(group, {}).update({object_id: [no_match_group, coords_tuple]})
    groups_list = [objects for (group, objects) in groups_dict.items()]

    #Map peer group calc to each group and output.
    #Uses each core of the processor.
    with open(output_file, 'w') as f:
        with futures.ProcessPoolExecutor() as pool:
            for peer_groups in pool.map(calc_a_peer_group, groups_list):
                write_peer_groups(f, peer_groups)


class PeerGroupTooSmall(Exception):
    '''Exception for when a peer group is below the minimum required.'''
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
