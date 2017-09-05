from __future__ import division

import pandas as pd
import numpy as np

from util import UTM
from util.distance import getdist


def interpolate(source_df, target_df, cutoff_km=15, method='invd', cv=False):
    """
    `source_df` should be a Series of the value to interpolate. Series index
      should be UTM plus time variables.

      `cv` - Cross-validate, throw out exact/very close distance matches.
    """

    # Reshape source to UTM by time
    source_wide = source_df.unstack(list(
        set(source_df.index.names).difference(set(UTM))
    ))
    # Drop source if it's missing values for some times (balance the panel)
    source_wide = source_wide[source_wide.notnull().all(axis=1)]
    # Get dists for each set of unique UTM's (just need source UTM in cols)
    dist = getdist(target_df,
                   source_wide.iloc[:, 0].to_frame('dum').reset_index())
    if cv:
        dist[dist <= 1e-3] = cutoff_km * 1.1

    assert dist.shape[1] == source_wide.shape[0]
    assert dist.shape[0] == target_df.shape[0]

    # Pass dist to invdist to get weights
    dist = dist.applymap(lambda x: 1 / x if x < cutoff_km else 0)  # Invert dist
    dist = dist.divide(dist.sum(axis=1), axis=0)    # Re-scale to 1

    # Multiply weight matrix by wide source_df: done!
    interpolated = pd.DataFrame(np.dot(dist.values, source_wide.values),
                                columns=source_wide.columns,
                                index=dist.index)

    return interpolated


if __name__ == '__main__':
    test_source = pd.DataFrame(np.array([
        [34000, 31000, 1990, 3.4],
        [34000, 31000, 1991, 4.4],
        [34200, 31200, 1990, 3.1],
        [34200, 31200, 1991, 3.9]]),
        columns=UTM + ['year', 'val']).set_index(UTM + ['year']).squeeze()
    test_target = pd.DataFrame(np.array([
        [33000, 31000],
        [34000, 32000],
        [34100, 31250],
        [32200, 37200]]),
        columns=UTM).set_index(UTM)
    wut = interpolate(test_source, test_target)
