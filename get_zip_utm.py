from __future__ import division

import numpy as np
import pandas as pd
import pyproj

from econtools import load_or_build


@load_or_build('../data/zip4_utm.dta')
def zip4_utms():
    """ Create table of ZIP+4 utm (real) and utm rounded to 100 m. """
    df = pd.read_stata('../data/ZIP4.dta')
    df = df.drop('zip4', axis=1)
    df = df.rename(columns={'zip4n': 'zip4'})

    zip4 = df[['zip4', 'lon', 'lat']].copy()

    utmz11 = pyproj.Proj(proj='utm', zone=11, ellps='WGS84')
    zip4['utm_east_real'], zip4['utm_north_real'] = utmz11(zip4['lon'].values,
                                                           zip4['lat'].values)
    for utm in ('utm_east', 'utm_north'):
        n_real = utm + '_real'
        zip4[n_real] = zip4[n_real].astype(np.int64)
        zip4[utm] = (np.around(zip4[n_real] / 100) * 100).astype(np.int64)

    zip4 = zip4.drop(['lon', 'lat'], axis=1)

    zip4 = zip4.set_index('zip4')

    return zip4


if __name__ == '__main__':
    df = zip4_utms()
