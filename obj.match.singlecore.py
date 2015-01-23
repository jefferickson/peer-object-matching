#! /usr/bin/env python3

##########################################
# Author:   Jeff Erickson <jeff@erick.so>
# Date:     2014-12-26
##########################################

# USAGE: ./obj.match.singlecore.py INPUT_FILE OUTPUT_FILE

import csv
import math
import heapq
from datetime import datetime
import sys


def euclid_distance(coords1, coords2):
    '''Calculates the Euclidean distance between two points of equal dimension.'''

    assert len(coords1) == len(coords2), "ERROR: the tuples are not the same length!"
    sum = 0
    for x in range(len(coords1)):
        sum += (coords1[x] - coords2[x]) ** 2

    return math.sqrt(sum)

def write_peer_group(f, object_id, peer_ids, delimiter = ','):
    '''Writes the object_id and the object_ids of the peers to file.'''

    f.write(object_id)
    for peer_id in sorted(peer_ids):
        f.write(delimiter + peer_id)
    f.write('\n')

def read_objects_and_group(input_file, delimiter = ','):
    '''Reads and groups objects (with coords) from file.'''
    
    groups = {}
    with open(input_file) as f: 
        reader = csv.reader(f, delimiter = delimiter)
        for row in reader:
            object_id, group, no_match_group, *coords = row
            coords_tuple = tuple([float(x) for x in coords])
            groups.setdefault(group, []).append((object_id, no_match_group, coords_tuple))
    
    return groups

def calc_peer_groups_and_output(groups, output_file, delimiter = ',', max_peer_group_n = 100, min_peer_group_n = None, max_distance_allowed = None):
    '''Iterates over each group and within each group, over each pair of objects. Calculates the distance between the two objects and writes the peer group to file.'''
    
    with open(output_file, 'w') as f:
        groups_n = len(groups)
        cur_group_num = 1

        for group, objects in groups.items():
            objects_n = len(objects)
            cur_object_num = 1

            for object1_tuple in sorted(objects):
                sys.stdout.write('\r\x1b[2KGROUP: {}/{}, OBJECT: {}/{}'.format(cur_group_num, groups_n, cur_object_num, objects_n))
                object1, object1_no_match_group, coords1 = object1_tuple
                distances = []

                for object2_tuple in objects:
                    object2, object2_no_match_group, coords2 = object2_tuple
                    if object1 == object2: continue
                    # If a no_match_group is defined, make sure it doesn't match.
                    if object1_no_match_group and object1_no_match_group == object2_no_match_group: continue
                    try:
                        distance_between_objects = euclid_distance(coords1, coords2)
                    except TypeError as e:
                        print('Either {} or {} has invalid coordinates.'.format(object1, object2))
                    if not max_distance_allowed or distance_between_objects <= max_distance_allowed:
                        distances.append({'object_id': object2, 'distance': distance_between_objects})
                
                peer_group = heapq.nsmallest(max_peer_group_n, distances, key = lambda s: s['distance'])
                if min_peer_group_n and len(peer_group) < min_peer_group_n:
                    raise PeerGroupTooSmall('{} has too few peers.'.format(object1))
                peer_ids = [peer_object['object_id'] for peer_object in peer_group]
                write_peer_group(f, object1, peer_ids, delimiter)
                
                cur_object_num += 1
            
            cur_group_num += 1


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
        sys.exit('USAGE: ./obj.match.singlecore.py INPUT_FILE OUTPUT_FILE')

    #Read in data and group.
    groups = read_objects_and_group(input_file)

    #Calc the peer groups and output to file.
    try:
        calc_peer_groups_and_output(groups, output_file)
    except PeerGroupTooSmall as e:
        print(e.args[0])

    #How long did it take?
    print(datetime.now() - start_time)
