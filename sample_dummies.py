"""
Create flags for "ZIP+4 is in the coastal sample."
"""
import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist

from econtools import load_or_build

from get_zip_utm import zip4_utms


UTM = ['utm_east', 'utm_north']


@load_or_build('../data/zips_coast_flag.dta')
def get_zip4_coast_flag():
    zip_utm = zip4_utms()

    samp = center_sw(zip_utm, maxdist=10)
    samp = samp.set_index('zip4')
    samp['samp_std'] = 1

    zip_utm = zip_utm.join(samp[['samp_std']], on='zip4', how='left')
    zip_utm['samp_std'] = zip_utm['samp_std'].fillna(0).astype(int)

    # Do it again for convex coastal samp
    samp = center_sw(zip_utm, maxdist=10, convex=True)
    samp = samp.set_index('zip4')
    samp['samp_convex'] = 1

    zip_utm = zip_utm.join(samp[['samp_convex']], on='zip4', how='left')
    zip_utm['samp_convex'] = zip_utm['samp_convex'].fillna(0).astype(int)

    zip_samp = zip_utm.set_index('zip4')[['samp_std', 'samp_convex']].copy()

    return zip_samp


def center_sw(regdata, maxdist=10, convex=False):
    df = regdata.reset_index()
    region_points = get_region_points()
    _push_out_southwest(region_points)
    # Main "distance from seed firms" criterion
    in_region = getdist(df, region_points, within=maxdist).max(axis=1)

    if convex:
        # Add area between circles
        edge_seeds = (9755, 800123, 115394)
        for i in range(len(edge_seeds) - 1):
            firm1, firm2 = edge_seeds[i:i+2]
            tx1, tx2, tanline = _tan_data(region_points, firm1, firm2, maxdist)
            below_line = (
                (df['utm_north'] <= tanline(df['utm_east'])) &
                (df['utm_north'] >=
                 region_points.loc[[firm1, firm2], 'utm_north'].min())
            )
            between_points = (
                (df['utm_east'] >= tx1) & (df['utm_east'] <= tx2))
            in_hull = below_line & between_points
            in_region[in_hull.values] = True

    # Restrict
    regdata = regdata[in_region.values].copy()

    return regdata

def _push_out_southwest(df):
    df.loc[800335, :] -= 3000

def _tan_data(df, firm1, firm2, maxkm):
    r = maxkm * 1000
    x1, y1 = df.loc[firm1, UTM].tolist()
    x2, y2 = df.loc[firm2, UTM].tolist()
    # Slope of tangent line (when r is equal, slope if tan line is slope of
    # center line)
    m = (y2 - y1) / (x2 - x1)
    tx1 = __tan_x(m, x1, r)
    tx2 = __tan_x(m, x2, r)

    b = __tan_intercept(tx1, m, r, x1, y1)
    assert b == __tan_intercept(tx2, m, r, x2, y2)

    def theline(x):
        return m * x + b

    return tx1, tx2, theline

def __tan_x(m, x0, r):
    return - m * r / np.sqrt(1 + m ** 2) + x0

def __tan_intercept(tx, m, r, x, y):
    return __circle(tx, r, x, y) - m * tx

def __circle(x0, r, x, y):
    return np.sqrt(r ** 2 - (x0 - x) ** 2) + y


def get_region_points():
    df = pd.DataFrame(
        [[9755, 371543, 3757044],
         [14052, 370811, 3746606],
         [18763, 368598, 3752689],
         [115314, 386429, 3736456],
         [115394, 397893, 3737207],
         [800074, 398811, 3736070],
         [800075, 368345, 3754078],
         [800123, 386809, 3747521],
         [800170, 382803, 3737444],
         [800335, 374952, 3743957]],
        columns=['facid', 'utm_east', 'utm_north'],
    )
    df = df.set_index('facid')
    return df


def getdist(base, target, within=0):
    distDF = pd.DataFrame(
        cdist(base[UTM].values / 1000, target[UTM].values / 1000),
        columns=target.index,
    )
    distDF.index = pd.MultiIndex.from_arrays(
        [base['utm_east'], base['utm_north']])

    if within > 0:
        distDF = distDF <= within

    return distDF


if __name__ == '__main__':
    df = get_zip4_coast_flag()
