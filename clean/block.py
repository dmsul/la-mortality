from os import path, chdir, getcwd
import subprocess

import pandas as pd
import numpy as np
import pyproj
import fiona

from econtools import load_or_build, force_iterable

from util import UTM
from util.gis import utmz11
from util.system import data_path, src_path

# File stem for linefile and shapefile
RECORDS_FILE = 'TGR{}.{}'

FIPS_LIST = ['06037', '06059', '06065', '06071']


@load_or_build(data_path('blocks2000_zips.p'))
def load_blockzips():
    """BlockID zip xwalk"""
    zips = get_block_zip_xwalk(FIPS_LIST)
    return zips


@load_or_build(data_path('blocks2000.p'))
def load_blocks():
    """Blocks with UTM and population"""
    pop = censuspop(FIPS_LIST)
    utm = blocksUTM(FIPS_LIST)
    blocks = pop.join(utm)
    return blocks


def load_blocks_wzip(nonzero=True, **kwargs):
    """
    Blocks' UTM, pop, and zip.

    `nonzero` bool, (True): Return only blocks with non-missing ZIP and nonzero
    population.

    `kwargs` get passed to `load_or_build`.
    """
    df = _load_blocks_wzip_guts(**kwargs)

    if nonzero:
        df = df[(df['zip'].notnull()) & (df['pop2000'] != 0)].copy()

    return df


@load_or_build(data_path('blocks_wzip.p'))
def _load_blocks_wzip_guts():

    blocks = load_blocks()
    zips = load_blockzips()
    joined = blocks.join(zips)

    return joined


def get_block_zip_xwalk(fips_list):
    """ RT1 is the master table. RT6 is 'additional zip codes.' """

    record_list = ['RT1', 'RT6']

    _unzip_tigerline(fips_list, record_list)

    fips_dfs = []

    for fips in fips_list:
        record_dfs = dict()
        for record in record_list:
            record_dfs[record] = fipss_tigerline2df(fips, record)

        fips_df = pd.merge(
            record_dfs['RT1'], record_dfs['RT6'], on=['tlid', 'side'],
            how='left')

        fips_df['blockID'] = fips_df.apply(_gen_zips_blockid,
                                           args=(fips,), axis=1)

        fips_df = pd.melt(fips_df, id_vars=['blockID'],
                          value_vars=['zip_RT1', 'zip_RT6'], value_name='zip')

        fips_df = fips_df[['blockID', 'zip']].dropna()
        fips_df = fips_df.drop_duplicates()

        fips_df['zip'] = fips_df['zip'].astype(np.int32)

        fips_dfs.append(fips_df)

    df = pd.concat(fips_dfs, ignore_index=True)
    df = df.set_index('blockID')

    return df

def _unzip_tigerline(fips_list, record_list):   #noqa

    fips_list = force_iterable(fips_list)
    record_list = force_iterable(record_list)

    tigerline_path = src_path('tigerline2000')

    cwd = getcwd()
    chdir(tigerline_path)

    try:
        for fips in fips_list:
            zip_file = 'tgr{}.zip'.format(fips)
            for record in record_list:
                record_file = RECORDS_FILE.format(fips, record)
                if path.isfile(path.join(tigerline_path, record_file)):
                    continue
                else:
                    subprocess.call(['unzip', zip_file, record_file])
    except:
        chdir(cwd)
        raise

    chdir(cwd)

def fipss_tigerline2df(fips, record):   #noqa
    RT_widths = {
        'RT1': [1, 4, 10, 1, 1, 2, 30, 4, 2, 3, 11, 11, 11, 11, 1, 1, 1, 1, 5,
                5, 5, 5, 1, 1, 1, 1, 2, 2, 3, 3, 5, 5, 5, 5, 5, 5, 6, 6, 4, 4,
                10, 9, 10, 9],
        'RT6': [1, 4, 10, 3, 11, 11, 11, 11, 1, 1, 1, 1, 5, 5]
    }

    RT_names = {
        'RT1': ['rt', 'version', 'tlid', 'side1', 'source', 'fedirp', 'fename',
                'fetype', 'fedirs', 'cfcc', 'fraddl', 'toaddl', 'fraddr',
                'toaddr', 'friaddl', 'toiaddl', 'friaddr', 'toiaddr', 'zipl',
                'zipr', 'aianhhl', 'aianhhr', 'aihhtlil', 'aihhtlir', 'census1',
                'census2', 'statel', 'stater', 'countyl', 'countyr', 'cousubl',
                'cousubr', 'submcdl', 'submcdr', 'placel', 'placer', 'tractl',
                'tractr', 'blockl', 'blockr', 'frlong', 'frlat', 'tolong',
                'tolat'],

        'RT6': ['rt', 'version', 'tlid', 'rtsq', 'fraddl', 'toaddl', 'fraddr',
                'toaddr', 'friaddl', 'toiaddl', 'friaddr', 'toiaddr', 'zipl',
                'zipr']
    }

    RT_usecols = {
        'RT1': ['tlid', 'tractr', 'tractl', 'blockl', 'blockr', 'zipl', 'zipr'],
        'RT6': ['tlid', 'zipl', 'zipr']
    }

    file_path_model = src_path('tigerline2000', RECORDS_FILE)

    file_path = file_path_model.format(fips, record)

    # Read raw record file
    df = pd.read_fwf(file_path,
                     widths=RT_widths[record],
                     names=RT_names[record],
                     header=None,
                     usecols=RT_usecols[record])
    df.set_index('tlid', inplace=True)

    # Each variable has "left" and "right" side of `line` object.
    # Pivot data 'long' in 'side' for easier handling later
    df.columns = pd.MultiIndex.from_tuples(
        [(x[:-1], x[-1:]) for x in df.columns],
        names=['var', 'side'])
    df = df.stack('side').reset_index().drop_duplicates()

    # Rename zip for easy merging later
    df.rename(columns={'zip': 'zip_{}'.format(record)}, inplace=True)

    return df

def _gen_zips_blockid(x, fips):     #noqa
    tract = str(int(x['tract'])).zfill(6)
    block = str(int(x['block'])).zfill(4)
    return '{}{}{}'.format(fips, tract, block)


def censuspop(fips_list):

    nhgis_path = src_path('census', 'nhgis0009_ds147_2000_block.csv')

    usecols = ['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA', 'FXS001']
    df = pd.read_csv(nhgis_path, usecols=usecols, header=0)

    # Restrict to fips_list
    df['fips'] = df.apply(_gen_fips, axis=1)
    df = df[df['fips'].isin(fips_list)]

    df['blockID'] = df.apply(_gen_pop_blockID, axis=1)

    df = df.rename(columns={'FXS001': 'pop2000'})
    df = df[['blockID', 'pop2000']]

    df = df.set_index('blockID')

    return df

def _gen_fips(x):       #noqa
    fips = str(x['STATEA']).zfill(2) + str(x['COUNTYA']).zfill(3)
    return fips

def _gen_pop_blockID(x):    #noqa
    tract = str(x['TRACTA']).zfill(6)
    block = str(x['BLOCKA']).zfill(4)
    return '{}{}{}'.format(x['fips'], tract, block)


def blocksUTM(fips_list):
    shp_path_model = src_path('shapefiles', 'ca_block2000',
                              'tl_2010_{}_tabblock00.shp')

    keep_properties = ['BLKIDFP00', 'INTPTLAT00', 'INTPTLON00']

    df_list = []
    for fips in fips_list:
        with fiona.open(shp_path_model.format(fips)) as features:
            # Get shapefile metadata into DataFrame
            fips_df = pd.DataFrame(
                [[polygon['properties'][prop] for prop in keep_properties]
                    for polygon in features],
                columns=['blockID', 'lat', 'lon'])
            # Convert from shape's data projection to WGS84, UTM, zone 11
            fips_proj = pyproj.Proj(features.crs)
            fips_df['utm_east'], fips_df['utm_north'] = pyproj.transform(
                fips_proj, utmz11, fips_df['lon'].values, fips_df['lat'].values)
            del fips_df['lon'], fips_df['lat']

        df_list.append(fips_df)

    df = pd.concat(df_list, ignore_index=True)

    df[UTM] = np.around(df[UTM]).astype(int)

    df = df.set_index('blockID')

    return df
