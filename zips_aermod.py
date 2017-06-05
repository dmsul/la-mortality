import pandas as pd
import numpy as np


def load_zips_aermod_pre():
    df = pd.read_stata('../data/zips_aermod.dta')
    df = df.set_index('zip4')

    pre_cols = ['aermod_1995q1', 'aermod_1995q2', 'aermod_1995q3',
                'aermod_1995q4', 'aermod_1996q1', 'aermod_1996q2',
                'aermod_1996q3', 'aermod_1996q4']
    aermod_pre = df[pre_cols].mean(axis=1).to_frame('aermod_pre')

    if 1:
        aermod_pre.to_stata('../data/zips_aermod_pre.dta')

    return aermod_pre


def load_zips_aermod():
    df = grids_wide()
    utm = pd.read_stata('../data/zip4_utm.dta')

    utm = utm.set_index(['utm_east', 'utm_north'])
    utm = utm.drop(['utm_east_real', 'utm_north_real'], axis=1)

    zips_aermod = df.join(utm, how='inner')

    zips_aermod = zips_aermod.set_index('zip4')

    zips_aermod = zips_aermod.astype(np.float32)

    if 1:
        zips_aermod.to_stata('../data/zips_aermod.dta')

    return zips_aermod


def grids_wide():
    grids = pd.read_pickle('../data/grids_aermod.pkl')
    df = grids.unstack('quarter')

    new_cols = ['{}_{}q{}'.format(x[1], x[0], x[2]) for x in df.columns]
    df.columns = new_cols

    return df


if __name__ == '__main__':
    # df = load_zips_aermod()
    df = load_zips_aermod_pre()
