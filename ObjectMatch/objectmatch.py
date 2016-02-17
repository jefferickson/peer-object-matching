import csv
from functools import partial
from concurrent import futures
from datetime import datetime

import ObjectMatch.peering as peering
import ObjectMatch.utils as utils

import pyximport; pyximport.install()
from ObjectMatch.distance_functions import euclid_distance


class ObjectMatch:
    '''
    Class: ObjectMatch
    Desc: Used to peer objects based on discrete and continous characteristics. See README.

    INPUT FILE FORMAT: object_id, categorical_group, no_match_group, continuous_data_1, continuous_data_2, ..., continuous_data_n
    OUTPUT FILE FORMAT: object_id, peer_object_id_1, peer_object_id_2, ..., peer_object_id_m
    '''

    def __init__(self):

        # File names
        self.input_file = None # Required
        self.output_file = None # Required
        self.lag_file = None
        self.delimiter = ','
        # Write diagnostics to file?
        self.diag_file = None

        # Parallelization settings
        self.max_workers = None
        self.max_group_size = 5000

        # Peer group settings
        self.distance_func = euclid_distance
        self.break_ties_func = utils._hash_string # None for random
        self.max_distance_allowed = None
        self.min_peer_group_n = None
        self.max_peer_group_n = None # Required, but can by set by self.run()
        self.dim_restrictions = {}

        # Internal data storage
        self._groups = {}
        self._lag_groups = {}

    def run(self, max_peer_group_n = None, retain_groups = False, time_it = True):
        '''Load object, groups, and coords from file, run peer group calc per group, and output.'''

        # keep track of time if time_it == True
        if time_it:
            start_time = datetime.now()

        # if max_peer_group_n supplied, override setting. Otherwise we can just use the one already set.
        if max_peer_group_n:
            self.max_peer_group_n = max_peer_group_n

        # Make sure required config is present
        self._self_test()

        # Load data from self.input_file
        self._groups = self._read_data_and_group(self.input_file)
        if not self._groups:
            raise utils.IncompleteConfiguration('Nothing to do! Nothing loaded from input file.')

        # If we have a lag_file, load those as well
        if self.lag_file:
            self._lag_groups = self._read_data_and_group(self.lag_file)
            if not self._lag_groups:
                raise utils.IncompleteConfiguration('Nothing to do! Lag file specified, but nothing loaded.')

        # We are going to utilize the Map-Reduce method. We're going break up each group into pieces, assigning
        # each to a processor thread. Upon return of the calculations, we will write out the results to file.
        # First, we need to partially apply our args before mapping.
        partial_calc_peers_for_group = partial(peering._calc_peers_for_group,
                                               distance_function = self.distance_func,
                                               max_distance_allowed = self.max_distance_allowed,
                                               break_ties_func = self.break_ties_func,
                                               max_peer_group_n = self.max_peer_group_n,
                                               min_peer_group_n = self.min_peer_group_n,
                                               dim_restrictions = self.dim_restrictions,
                                               diag = self.diag_file,
                                               )
        with open(self.output_file, 'w') as f:
            with futures.ProcessPoolExecutor(max_workers = self.max_workers) as pool:
                for peer_groups in pool.map(partial_calc_peers_for_group, self._generate_groups()):
                    utils._write_peer_groups(f, peer_groups, delimiter = self.delimiter)

        # Unless we retain_groups, assign None to the dicts now that we are done with them because they could be large and unneeded
        if not retain_groups:
            self._groups = None
            self._lag_groups = None

        if time_it:
            print(datetime.now() - start_time)

    def _self_test(self):
        '''Are we ready to self.run()? Check all config options required to make sure they are defined.'''

        if not all((self.input_file,
                    self.output_file,
                    self.max_peer_group_n,
                   )):
            raise utils.IncompleteConfiguration('Missing required configuration options. See README.')

        # Make sure max number of peers is >= min number of peers (if the latter exists)
        if self.min_peer_group_n and self.max_peer_group_n < self.min_peer_group_n:
            raise utils.IncompleteConfiguration('Config error: max peer group size smaller than min peer group size.')

    def _read_data_and_group(self, input_file):
        '''Read data from file, group, and store.'''

        # load in all of the data, storing it by the categorical groups because they must be exact matches on that data.
        groups_dict = {}
        with open(input_file) as f:
            reader = csv.reader(f, delimiter = self.delimiter)
            for row in reader:
                try:
                    object_id, group, no_match_group, *coords = row
                except ValueError as e:
                    print('Not enough values in row: {}'.format(row))

                # Convert to float and add to the dictionary
                coords_tuple = tuple(float(x) for x in coords)
                groups_dict.setdefault(group, []).append((object_id, no_match_group, coords_tuple))

        return groups_dict

    def _generate_groups(self):
        '''A generator that slices up each group into chunks to aid with CPU utilization. Yields a tuple of the subset and the whole group.
        If self._lag_groups exists, it is used as the whole group.'''

        for group, objects in self._groups.items():

            # If we are using self._lag_groups, then the peer_group should be the lag_group with the same group spec
            # Otherwise it should just be the objects themselves
            if self._lag_groups:
                peer_group = self._lag_groups.get(group, [])
            else:
                peer_group = objects

            # If diagnostics reporting, report "bin size"
            if self.diag_file:
                utils._write_diag(self.diag_file, 'bin_size', len(peer_group))

            for i in range(0, len(objects), self.max_group_size):
                # Yield a tuple of the m x n group to process. m rows with all n peer columns.
                yield objects[i:i+self.max_group_size], peer_group
