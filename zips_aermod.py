from __future__ import division

import pandas as pd
import numpy as np

from econtools import load_or_build

from get_zip_utm import zip4_utms
from atmods.io import load_full_exposure

@load_or_build('../data/zips_{}.dta', path_args=['model'])
def load_zips_exposure(model='aermod_nox'):
    df = grids_wide(model=model)
    utm = zip4_utms()

    utm = utm.set_index(['utm_east', 'utm_north'])
    utm = utm.drop(['utm_east_real', 'utm_north_real'], axis=1)

    zips_aermod = df.join(utm, how='inner')

    zips_aermod = zips_aermod.set_index('zip4')

    zips_aermod = zips_aermod.astype(np.float32)

    return zips_aermod


def grids_wide(model='aermod_nox'):
    df = load_full_exposure('grid', '{}'.format(model))
    QUARTERLY_MODELS = ('aermod_nox', 'invd15_nox', 'invd15_ozone',
                        'unif2_nox', 'tria5_nox')
    if model in QUARTERLY_MODELS:
        df = df.unstack('quarter')
        new_cols = ['{}_{}q{}'.format(x[1], x[0], x[2]) for x in df.columns]
    else:
        new_cols = ['{}_{}'.format(x[1], x[0]) for x in df.columns]

    df.columns = new_cols

    return df


if __name__ == '__main__':
    df = load_zips_exposure(model='tria5_nox')
