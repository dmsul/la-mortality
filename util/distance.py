"""
Distance tools.
"""
import numpy as np
import pandas as pd

from util import UTM


def center_data(df, target_utms, maxdist, grab=None):
    """Restrict `basedf` to the area within `maxdist` km of target points """

    # Get base's unique UTM
    try:
        bases_utm = df[UTM].drop_duplicates()
        utm_in_index = False
    except KeyError:
        bases_utm = df.reset_index(UTM)[UTM].drop_duplicates()
        utm_in_index = True

    dist_to_nn = getdist(bases_utm, target_utms).min(axis=1)
    distname = 'dist_to_nn'
    dist_to_nn.name = distname

    if utm_in_index:
        centered_df = df.join(dist_to_nn)
    else:
        centered_df = df.join(dist_to_nn, on=UTM)

    # Get `grab` group's min dist and use it instead
    grabname = 'grab_dist'
    if grab:
        weakest_link = centered_df.groupby(grab)[distname].min()
        weakest_link.name = grabname
        centered_df = centered_df.join(weakest_link, on=grab)
        drop_on = grabname
    else:
        drop_on = distname

    # Actual restriction
    centered_df = centered_df.loc[centered_df[drop_on] <= maxdist, :]

    # Clean up aux variables
    for colname in [distname, grabname]:
        try:
            centered_df.drop(colname, axis=1, inplace=True)
        except ValueError:
            pass
    # Replace columns (earlier join ruins multi-level cols)
    centered_df.columns = df.columns

    return centered_df


def getdist(base, target, within=0):
    base = base.copy()
    target = target.copy()

    if target.ndim == 1:
        target = pd.DataFrame(target).T

    float_type = np.float64
    if within > 0:
        datatype = bool
    else:
        datatype = float_type

    distDF = pd.DataFrame(
        np.zeros((base.shape[0], target.shape[0]), dtype=datatype),
        columns=target.index,
    )
    distDF.index = pd.MultiIndex.from_arrays(
        [base['utm_east'], base['utm_north']])

    for idx, row in target.iterrows():
        # Make sure values are 64-bit before squaring, or they'll go negative
        xtilde = (base['utm_east'] - row['utm_east']).astype(np.int64)
        ytilde = (base['utm_north'] - row['utm_north']).astype(np.int64)
        dist = np.sqrt(xtilde ** 2 + ytilde ** 2).values / 1000.

        if within > 0:
            dist = dist <= within

        distDF[idx] = dist

    return distDF


def nearestneighbor(base, target, return_dist=False):
    dist = getdist(base, target)

    targetID = dist.columns.name
    nn = pd.DataFrame(dist.idxmin(axis=1), columns=[targetID])

    if return_dist:
        nn[targetID + '_distkm'] = dist.min(axis=1)

    return nn
