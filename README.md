## Peer Object Matching

#### Author: Jeff Erickson `<jeff@erick.so>`
#### Date: 2014-12-28 (Updated: 2015-04-07)

### Overview

Use to match objects (peer objects) based on categorical and continuous data. First, the objects are matched exactly based on their categorical data, then they are matched within each categorical group based on the Euclidean distance of their continuous data. The min and max number of matches, group(s) _not_ to match on, and the max distance can also be specified.

The input should be of the following form:

`object_id, categorical_data, no_match_groups, cont_point_1, cont_point_2, ..., cont_point_n`

with one object per line. Leaving `no_match_group` as a blank field will cause all objects to be compared within the categorical group. Multiple groups not to match on can be separated by a '|' in this field.

The output will be:

`object_id, peer_object_id_1, peer_object_id_2, ..., peer_object_id_m`

### Categorical Data

The categorical data can be anything. Grouping will be done on the unique values of this field. This can be a group label (`group1`, `group2`, etc.), concatenated categorical fields (`x:y` where `x` and `y` are different categorical flags), etc. While any number of data can be used, they must be concatenated into one field.

### Continuous Data

Any number of continuous dimensions can be used, as long as each object has the same number of dimensions. Objects will be matched based within their categorical groups based on the shortest distance between objects. Euclidean distance in _n_ dimensions is used here, but other distance algorithms could be substituted.

### Lag Peers

It is possible to specify a separate list of objects from which to peer. The lag file must have the same format as the input file. The objects in the lag file will be used as peers, but will not be peered themselves.

### Single-core [deprecated] vs. Multi-core

Because of improvements in the multi-core version, the single-core version [deprecated] is no longer necessary. For users that wish to only use one core (or wish to specify a maximum number of cores), an argument, `max_workers`, can be specified.

The script breaks up processing into "chucks" in order to maximum CPU utilization across available cores.

### Example

```
#! /usr/bin/env python3

# Import the class
import ObjectMatch as OM

if __name__ == '__main__':

    # Create new instance of the class
    match = OM.ObjectMatch()

    # Set (at least) the required settings
    match.input_file = 'input_data.csv'
    match.output_file = 'output_data.csv'
    match.diag_file = 'diag_info.csv' #optional, don't use when speed is a concern

    # And GO!
    match.run(100) #Create groups of (at most) 100
```

### Diagnostics

Specifying a diagnostic output file will output certain diagnostic information regarding the bins (the groups of matching categorical data) and peer groups created from the run. This diagnostic information can be easily visualized by running the included R Markdown script (`run-diags.Rmd`) in RStudio.
