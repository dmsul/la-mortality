from __future__ import division, print_function

import os

import numpy as np
import pandas as pd
import simpledbf
import fiona
from shapely.geometry import shape

from econtools import load_or_build

from util.system import data_path
from clean.io import load_cleand
from clean.house.raw import load_assessor


def patch_lotsize(in_df):
    """
    `lotsize` in original DataQuick assessor data is awful for randomly
    selected observations. This uses data straight from county assessor's
    websites to patch these observations when necessary.

    Note: This only covers LA and Orange counties b/c that's enough for the
    final sample area.
    """
    if 'parcel_nbr' in in_df.columns:
        keep_vars = ['property_id', 'parcel_nbr', 'bg', 'lotsize', 'sqft']
        df = in_df[keep_vars].drop_duplicates()
    else:
        keep_vars = ['property_id', 'bg', 'lotsize', 'sqft']
        df = in_df[keep_vars].drop_duplicates()
        df = in_df[['property_id', 'bg', 'lotsize', 'sqft']].drop_duplicates()
        df = df.join(_parcel_nbr(), on='property_id', how='inner')

    lotsize = _lotsize()    # XXX Drops duplicate parcel numbers (for ease)
    df = df.join(lotsize.set_index('parcel_nbr'), on='parcel_nbr',
                 how='left')
    # Leave-one-out means
    bg_size = df.groupby('bg').size()
    df = df.join(bg_size.to_frame('bg_size'), on='bg')
    df = df[df['bg_size'] > 5]
    cols = ['lotsize', 'area']
    for col in cols:
        df = _leave_one_out(df, col)

    # Calc diff from leave-one-out's
    for col1 in cols:
        for col2 in cols:
            eval_str = '{} - bg_{}'.format(col1, col2)
            df['d_{}{}'.format(col1[0], col2[0])] = df.eval(eval_str)
    df['d_l'] = np.abs(df[['d_ll', 'd_la']]).mean(axis=1)
    df['d_a'] = np.abs(df[['d_al', 'd_aa']]).mean(axis=1)
    area_closer = df['d_a'] < df['d_l']

    pdiff = (df['lotsize'] - df['area']) / df['area']
    bigdiff = np.abs(pdiff) > .25
    use_area = (
        ((bigdiff) & (area_closer)) |
        (df['lotsize'] == 0) |
        (df['lotsize'].isnull()) |
        (df['lotsize'] < df['sqft'])
    )

    df['new_lotsize'] = np.where(use_area, df['area'], df['lotsize'])

    merge_series = df.set_index('property_id')['new_lotsize']

    return merge_series

def _leave_one_out(df, col):
    wut = df.groupby('bg')[col].sum()
    df = df.join(wut.to_frame('bg_tot' + col), on='bg')
    df['bg_' + col] = (df['bg_tot' + col] - df[col]) / (df['bg_size'] - 1)
    del df['bg_tot' + col]
    return df

def _parcel_nbr():
    df = _parcel_nbr_aux('los angeles')
    df = df.append(_parcel_nbr_aux('orange'), ignore_index=True)
    df = df.set_index('property_id')
    return df

def _parcel_nbr_aux(county):
    df = load_assessor(county)
    df = df[['property_id', 'parcel_nbr']]
    return df

def _lotsize():
    df = load_lotsize('los angeles')
    df = df.append(load_lotsize('orange'), ignore_index=True)

    # XXX restrict to non-duplicated parcel numbers
    n = df.groupby('parcel_nbr').size()
    df = df.join(n.to_frame('n'), on='parcel_nbr')
    df = df[df['n'] == 1]
    del df['n']

    return df


# Direct lotsize methods (from shapefiles)
def load_lotsize(county_name, **kwargs):
    if county_name == 'los angeles':
        return load_lotsize_LA(**kwargs)
    elif county_name == 'orange':
        return load_lotsize_orange(**kwargs)
    else:
        raise ValueError("Bad county")

@load_or_build(data_path('tmp_los-angeles_lotsize.pkl'))
def load_lotsize_LA():
    filepath = os.path.join('d:\\data', 'parcels', 'los_angeles', 'Parcel.dbf')
    dbf = simpledbf.Dbf5(filepath, codec='ISO-8859-1')
    df = dbf.to_dataframe()
    df = df[['AIN', 'SHAPE_area']]

    # Drop invalid AIN's
    df = df[df['AIN'].notnull()]
    df = df[df['AIN'].str.isdigit()]
    df['AIN'] = df['AIN'].astype(np.int64)

    df.columns = ['parcel_nbr', 'area']

    return df

@load_or_build(data_path('tmp_orange_lotsize.pkl'))
def load_lotsize_orange():
    filepath = os.path.join('d:\\data', 'parcels', 'orange_ca',
                            'Parcel_polygons.shp')
    with fiona.open(filepath) as layer:
        area = pd.Series(
            {shp['properties']['OBJECTID']: shape(shp['geometry']).area
             for shp in layer}
        )
    xwalk = _orange_id_xwalk()
    df = area.to_frame('area').join(xwalk)
    return df

def _orange_id_xwalk():
    xwalk_path = os.path.join('d:\\data', 'parcels', 'orange_ca',
                              'ParcelAttribute.csv')
    xwalk = pd.read_csv(xwalk_path, delimiter='|',
                        dtype={0: np.int64, 1: object, 2: object})
    xwalk.columns = ['OBJECTID', 'parcel_nbr', 'drop']
    xwalk = xwalk.drop('drop', axis=1)

    xwalk = xwalk[xwalk['OBJECTID'] != 0]
    xwalk['parcel_nbr'] = xwalk['parcel_nbr'].str.replace('-', '')
    xwalk['parcel_nbr'] = xwalk['parcel_nbr'].fillna(0, downcast='infer')

    return xwalk.set_index('OBJECTID')


def patch_on_disk():
    """ Used for old data, may be obsolete """
    df = load_cleand('house')
    return patch_lotsize(df)
