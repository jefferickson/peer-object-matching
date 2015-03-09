#! /usr/bin/env python3

##########################################
# Author:   Jeff Erickson <jeff@erick.so>
# Date:     2014-12-28
##########################################

# USAGE: ./obj.match.multicore.py -i INPUT_FILE -o OUTPUT_FILE
# (For help, see ./obj.match.multicore.py -h)
# INPUT FILE FORMAT: object_id, categorical_group, no_match_group, continuous_data_1, continuous_data_2, ..., continuous_data_n
# OUTPUT FILE FORMAT: object_id, peer_object_id_1, peer_object_id_2, ..., peer_object_id_m

import sys
import csv
import math
import heapq
import argparse
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

def memoize_peers(f):
    '''A decorator to keep track of identical objects' peer groups so we can reuse them and not recalculate the whole group.'''

    # We'll save the peer groups already calculated here.
    memo = {}
    def helper(an_object_to_peer, whole_group, *args, **kwargs):
        '''Check to see if we've already done this comparison and store.'''

        _, object_no_match_group, object_coords = an_object_to_peer
        comparison_factors = (object_no_match_group, object_coords)

        # Have we already calculated a peer group for these comparison factors?
        # If we are not utilizing object_no_match_group, then we cannot use the memo since we must rerun for all (no possible exact match)
        if comparison_factors in memo:
            peers_tuple = memo[comparison_factors]
        else:
            peers_tuple = f(an_object_to_peer, whole_group, *args, **kwargs)
            if object_no_match_group:
                memo[comparison_factors] = peers_tuple
        
        peers, *diag_info = peers_tuple

        # If reporting diagnostics, then write out info
        if cmd_line_args.diag:
            write_diag(cmd_line_args.diag, 'object_peer_group_size', diag_info[0])
            write_diag(cmd_line_args.diag, 'object_avg_peer_dist', diag_info[1])

        return peers 
    
    return helper

def calc_peers_for_object(an_object_to_peer, whole_group, dist_formula = euclid_distance, 
                            max_peer_group_n = 100, min_peer_group_n = None, max_distance_allowed = None):
    '''Calculates the peers for a single object. Use in combo with @memoize_peers to utilize memoization.'''
    
    object_id, object_no_match_group, object_coords = an_object_to_peer
    distances = []

    for a_peer in whole_group:
        peer_object_id, peer_object_no_match_group, peer_object_coords = a_peer
        # Don't peer an object with itself.
        if object_id == peer_object_id: continue
        # If a no_match_group is defined, make sure it doesn't match.
        if object_no_match_group and object_no_match_group == peer_object_no_match_group: continue
        try:
            distance_between_objects = dist_formula(object_coords, peer_object_coords)
        except (TypeError, DiffNumOfDims) as e:
            print('Either {} or {} has invalid coordinates.'.format(object_id, peer_object_id))
        if not max_distance_allowed or distance_between_objects <= max_distance_allowed:
            distances.append((peer_object_id, distance_between_objects))
    
    # Let's find the closest objects using a heap. Ties broken with ID unless option `--dont-break-ties` is used.
    if cmd_line_args.dont_break_ties:
        # distance only
        key = lambda s: s[1]
    else:
        # distance, ties broken by ID
        key = lambda s: (s[1], s[0])

    peer_group = heapq.nsmallest(max_peer_group_n, distances, key = key)
    if min_peer_group_n and len(peer_group) < min_peer_group_n:
        raise PeerGroupTooSmall('{} has too few peers.'.format(object_id))
    peer_ids = {peer_object[0] for peer_object in peer_group}

    # For diagnostics, calc average peer distance and number of peers (keep as None for speed if cmd_line_args.diag == None)
    n_peers = None
    avg_peer_dist = None
    if cmd_line_args.diag:
        n_peers = len(peer_group)
        avg_peer_dist = sum(peer_object[1] for peer_object in peer_group) / n_peers

    return peer_ids, n_peers, avg_peer_dist

def write_peer_groups(f, peer_groups, delimiter = ','):
    '''Writes the object_id and the object_ids of the peers to file.'''

    for object_id, peer_ids in sorted(peer_groups.items()):
        f.write(object_id)
        for peer_id in sorted(peer_ids):
            f.write(delimiter + peer_id)
        f.write('\n')

def write_diag(filename, label, diag, delimiter = ','):
    '''Write the diagnostics out to file.'''

    with open(filename, 'a') as f:
        f.write(label + delimiter + str(diag) + '\n')

def calc_peers_for_group(subset_and_whole_group_tuple, **kwargs_for_dist_calc):
    '''For a given dict of objects, calculates peer groups for each object compared to the other objects.'''

    # Break up the tuple we got from the generator generate_groups().
    group_subset, whole_group = subset_and_whole_group_tuple
    
    # Iterate over each object in subset, compare it to all other objects, and then find the closest peers.
    peer_groups = {}

    # Let's memoize the peer group calc function for this group to prevent repetition.
    calc_distance_for_this_group = memoize_peers(calc_peers_for_object)

    print("{}: Starting process with {} objects.".format(datetime.now(), len(group_subset)))

    for an_object_to_peer in group_subset:
        object_id, *_ = an_object_to_peer
        peer_ids = calc_distance_for_this_group(an_object_to_peer, whole_group, **kwargs_for_dist_calc)

        # Finally add them to our dictionary of results for this subset.
        peer_groups.update({object_id: peer_ids})

    print("{}: Ending process with {} objects.".format(datetime.now(), len(group_subset)))

    # Return the peer group dict to be written to file.
    return peer_groups

def load_calc_output_all_peer_groups(input_file, output_file, delimiter = ',', max_workers = None, max_group_size = 5000):
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
            
            coords_tuple = tuple(float(x) for x in coords)
            groups_dict.setdefault(group, []).append((object_id, no_match_group, coords_tuple))
    # Once everything is grouped by categorical identifiers, we can just pull out each group into a list 
    # (we don't need the categorical data anymore as it is already grouped.)
    groups_list = list(groups_dict.values())

    # We are going to utilize the Map-Reduce method. We're going break up each group into pieces, assigning
    # each to a processor thread. Upon return of the calculations, we will write out the results to file.
    with open(output_file, 'w') as f:
        with futures.ProcessPoolExecutor(max_workers = max_workers) as pool:
            for peer_groups in pool.map(calc_peers_for_group, generate_groups(groups_list, max_group_size = max_group_size)):
                write_peer_groups(f, peer_groups)

def generate_groups(groups, max_group_size = 5000):
    '''A generator that slices up each group into chunks to aid with CPU utilization. Yields a tuple of the subset and the whole group.'''
    
    for group in groups:

        # If diagnostics reporting, report "bin size"
        if cmd_line_args.diag:
            write_diag(cmd_line_args.diag, 'bin_size', len(group))

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

    #Add and parse arguments using argparse.
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help = 'Input filename.', required = True)
    parser.add_argument('-o', '--output', help = 'Output filename.', required = True)

    #Optional
    parser.add_argument('-w', '--workers', help = 'Set max number of workers to use for concurrent processing.', default = None, type = int)
    parser.add_argument('-g', '--max_group_size', help = 'Set max group size per process.', default = 5000, type = int)
    parser.add_argument('-d', '--diag', help = 'Output diagnostic info to file.', default = None)
    parser.add_argument('--dont-break-ties', help = 'Use to prevent the use of peer_object_id from breaking distance ties.', action = 'store_true')

    #Parse
    cmd_line_args = parser.parse_args()

    #Do the loading, calculations, and output.
    #If not using no_match_groups, recommend 1000 for max_group_size.
    #Otherwise 5000 has been working well.
    #Too low and you may not be taking advantage of the memoize closure.
    #Too high and you may not be utilizing CPU 100%.
    load_calc_output_all_peer_groups(cmd_line_args.input, cmd_line_args.output,
                                        max_workers = cmd_line_args.workers, 
                                        max_group_size = cmd_line_args.max_group_size)

    #How long did it take?
    print(datetime.now() - start_time)
