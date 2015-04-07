#! /usr/bin/env python3

##########################################
# Author:   Jeff Erickson <jeff@erick.so>
# Date:     2014-12-28 (Updated 2015-03-31)
##########################################

from datetime import datetime
import heapq

import ObjectMatch.utils as utils


def _calc_peers_for_group(subset_and_whole_group_tuple, **kwargs):
    '''For a given dict of objects, calculates peer groups for each object compared to the other objects.'''

    # Break up the tuple we got from the generator self._generate_groups().
    group_subset, whole_group = subset_and_whole_group_tuple

    # Iterate over each object in subset, compare it to all other objects, and then find the closest peers.
    peer_groups = {}

    # Let's memoize the peer group calc function for this group to prevent repetition.
    calc_distance_for_this_group = utils._memoize_peers(_calc_peers_for_object)

    print("{}: Starting process with {} objects.".format(datetime.now(), len(group_subset)))

    for an_object_to_peer in group_subset:
        object_id, *_ = an_object_to_peer

        peer_ids = calc_distance_for_this_group(an_object_to_peer, whole_group, **kwargs)
        # Finally add them to our dictionary of results for this subset.
        peer_groups.update({object_id: peer_ids})

    print("{}: Ending process with {} objects.".format(datetime.now(), len(group_subset)))

    # Return the peer group dict to be written to file.
    return peer_groups

def _calc_peers_for_object(an_object_to_peer, whole_group, *,
                            distance_function = utils._euclid_distance,
                            max_distance_allowed = None,
                            break_ties_func = utils._hash_string,
                            max_peer_group_n,
                            min_peer_group_n = None,
                            diag = None
                            ):
    '''Calculates the peers for a single object. Use in combo with @memoize_peers to utilize memoization.'''

    object_id, object_no_match_group, object_coords = an_object_to_peer
    distances = []

    for a_peer in whole_group:
        peer_object_id, peer_object_no_match_group, peer_object_coords = a_peer

        try:
            # Don't peer an object with itself.
            if object_id == peer_object_id:
                raise utils.DoNotPeer('Do not peer an object with itself.')
            # If a no_match_group is defined, make sure it doesn't match.
            # Multiple no match groups can be separated with a '|'
            if object_no_match_group:
                object_no_match_groups = object_no_match_group.split('|')
                peer_object_no_match_groups = peer_object_no_match_group.split('|')
                for i, group in enumerate(object_no_match_groups):
                    try:
                        if group == peer_object_no_match_groups[i]:
                            raise utils.DoNotPeer('Object within no_match_group.')
                    except IndexError as e:
                        print('{} has different number of no match characteristics.'.format(peer_object_id))
            # Calculate distance
            try:
                distance_between_objects = distance_function(object_coords, peer_object_coords)
            except (TypeError, utils.DiffNumOfDims) as e:
                print('Either {} or {} has invalid coordinates.'.format(object_id, peer_object_id))
            if max_distance_allowed and distance_between_objects > max_distance_allowed:
                raise utils.DoNotPeer('Distance between object exceeds maximum allowed.')
        except utils.DoNotPeer as e:
            # If raised self.DoNotPeer exception, then continue to next object
            continue
        # If everything went well, append to the list of peer options
        distances.append((peer_object_id, distance_between_objects))

    # Let's find the closest objects using a heap. Ties on dist, ID broken with break_ties_func if def
    if break_ties_func:
        # distance, ties broken by ID
        key = lambda s: (s[1], break_ties_func(s[0]))
    else:
        # distance only
        key = lambda s: s[1]

    peer_group = heapq.nsmallest(max_peer_group_n, distances, key = key)
    if min_peer_group_n and len(peer_group) < min_peer_group_n:
        raise utils.PeerGroupTooSmall('{} has too few peers.'.format(object_id))
    peer_ids = {peer_object[0] for peer_object in peer_group}

    # For diagnostics, calc average peer distance and number of peers (keep as None for speed if self.diag_file == None)
    n_peers = None
    avg_peer_dist = None
    if diag:
        n_peers = len(peer_group)
        avg_peer_dist = sum(peer_object[1] for peer_object in peer_group) / n_peers

    return peer_ids, n_peers, avg_peer_dist
