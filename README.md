## Peer Object Matching

#### Author: Jeff Erickson `<jeff@erick.so>`
#### Date: 2014-12-28

### Overview

Use to match objects (peer objects) based on categorical and continuous data. First, the objects are matched exactly based on their categorical data, then they are matched within each categorical group based on the Euclidean distance of their continuous data. The min and max number of matches as well as the max distance can be specified.

The input should be of the following form:

`object_id, categorical_data, cont_point_1, cont_point_2, ..., cont_point_n`

with one object per line.

The output will be:

`object_id, peer_object_id_1, peer_object_id_2, ..., peer_object_id_n`

### Categorical Data

The categorical data can be anything. Grouping will be done on the unique values of this field. This can be a group label (`group1`, `group2`, etc.), concatenated categorical fields (`x:y` where `x` and `y` are different categorical flags), etc. While any number of data can be used, they must be concatenated into one field.

### Continuous Data

Any number of continuous dimensions can be used, as long as each object has the same number of dimensions. Objects will be matched based within their categorical groups based on the shortest distance between objects. Euclidean distance in _n_ dimensions is used here, but other distance algorithms could be substituted.

### Single-core vs. Multi-core

Two versions are provided: `obj.match.simplecore.py` and `obj.match.multicore.py`. The former uses only one CPU core and runs slower as a result. The second version employs parallel programming and uses as many cores as are available. It should be noted that this is not the only difference. The former file is more memory efficient on one core, so if running them on a single core machine, they are not equivalent and the single core version should be used.

### Sample Input Data

The included R script (`create-sample-input-data.R`) can be used to create sample input files. The example provided creates fake students with random characteristics. The output would then be students matched with other students on those characteristics.

### Performance

Using sample data of 80,000 objects, 16 categorical groups (4 binary flags), and 4 continuous dimensions, the single core version runs in 23 minutes and the multicore version runs in 10 minutes on a quad-core machine.
