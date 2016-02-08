import math
import hashlib


def _write_peer_groups(f, peer_groups, delimiter = ','):
    '''Writes the object_id and the object_ids of the peers to file.'''

    for object_id, peer_ids in sorted(peer_groups.items()):
        f.write(object_id)
        for peer_id in sorted(peer_ids):
            f.write(delimiter + peer_id)
        f.write('\n')

def _write_diag(diag_file, label, diag, delimiter = ','):
    '''Write the diagnostics out to file.'''

    with open(diag_file, 'a') as f:
        f.write(label + delimiter + str(diag) + '\n')

def _memoize_peers(f):
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
        if 'diag' in kwargs and kwargs['diag']:
            _write_diag(kwargs['diag'], 'object_peer_group_size', diag_info[0])
            _write_diag(kwargs['diag'], 'object_avg_peer_dist', diag_info[1])
            _write_diag(kwargs['diag'], 'bin_size|object_avg_peer_dist',
                                        '|'.join((str(len(whole_group)), str(diag_info[1]))))

        return peers

    return helper

def _euclid_distance(coords1, coords2):
    '''Calculates the Euclidean distance between two points of equal dimension.'''

    if len(coords1) != len(coords2):
        raise DiffNumOfDims('ERROR: the tuples are not the same length!')

    sum = 0
    for x in range(len(coords1)):
        sum += (coords1[x] - coords2[x]) ** 2

    return math.sqrt(sum)

def _hash_string(string):
    '''Calculate hash of a string (encoded as UTF-8).'''

    return hashlib.md5(string.encode('utf-8')).hexdigest()

class PeerGroupTooSmall(Exception):
    '''Exception for when a peer group is below the minimum required.'''
    pass

class DiffNumOfDims(Exception):
    '''Exception for when two tuples have a different number of dimensions.'''
    pass

class DoNotPeer(Exception):
    '''Exception for when two objects should not be peered together.'''
    pass

class IncompleteConfiguration(Exception):
    '''Exception for when not all required configures options are set.'''
    pass
