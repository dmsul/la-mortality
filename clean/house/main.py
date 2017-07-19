from __future__ import division, print_function

import numpy as np
import pandas as pd

from econtools import load_or_build, group_id

from util import UTM
from util.system import data_path
from util.gis import utmz11
from clean.cpi import cpi_quarter
from clean.house.raw import load_history, load_assessor, dq_src_path
from clean.house.lotsize import patch_lotsize


HGRID_SIZE = 100    # meters


@load_or_build(data_path('house_sample.pkl'))
def load_house_sample():
    df = _merge_hist_ass()

    # Patch lotsize
    df = _gen_full_bg_string(df)    # Required for `patch_lotsize`
    df = _set_weird_hedonics(df)    # Run this once before `patch_lotsize`
                                    # so `lotsize < 100` is replaced with
                                    # missing and gets filled in by
                                    # `patch_lotsize`. Run it again so the new
                                    # `lotsize` also gets cleaned.

    # Patch lotsize
    new_lotsize = patch_lotsize(df)
    df = df.join(new_lotsize, on='property_id')
    # `patch_lotsize` only covers LA and Orange counties
    df['new_lotsize'] = np.where(df['new_lotsize'].isnull(),
                                 df['lotsize'],
                                 df['new_lotsize'])
    df = df.rename(columns={'lotsize': 'old_lotsize',
                            'new_lotsize': 'lotsize'})

    # Cleaning
    df = _set_weird_hedonics(df)
    df = _clean_geocodes(df)
    df = _make_house_grid(df)

    # Drop variables
    drop_vars = ['site_city', 'site_zip', 'buyer', 'seller']  # , 'parcel_nbr']
    df = df.drop(drop_vars, axis=1)

    # Create variables and reorder
    df = _recast(df)
    df['lnp'] = np.log(df['p_real'])
    df['p'] = df['val_transfer']
    df = _reorder_cols(df)

    return df

def _merge_hist_ass():
    """ Merge keeping unsold properties """
    df = clean_history()

    # Load assessor and drop variables
    ass = load_assessor()
    address_vars = ['site_house_nbr', 'site_dir', 'site_street_name',
                    'site_suf', 'site_unit_val']
    other_drop_vars = ['construction_qlty', 'cool_code',
                       'heat_code', 'roof_code', 'assr_year', 'bad_address',
                       'bad_assessor']
    ass = ass.drop(address_vars + other_drop_vars, axis=1)
    ass = ass[ass['use_code_std'] == 1]
    ass = ass.drop('use_code_std', axis=1)

    # Join (can't 'merge' because that wrecks dtypes)
    joined = ass.join(df.set_index('property_id'), on='property_id')

    # Check sales hit (no sales prop_id's lost)
    tmp = joined[['property_id']].drop_duplicates().set_index('property_id')
    assert df['property_id'].isin(tmp.index).min()
    del tmp

    return joined

def _set_weird_hedonics(df):
    for col in ('beds', 'baths', 'rooms', 'stories'):
        df.loc[df[col] == 0, col] = np.nan
    df.loc[df['sqft'] < 200, 'sqft'] = np.nan
    df.loc[df['lotsize'] < 100, 'lotsize'] = np.nan
    return df

def _gen_full_bg_string(df):
    df['bg'] = (
        '06' +
        '0' + df['mm_fips_muni_code'].astype(str) +
        df['census_tract'].astype(str).str.zfill(6) +
        df['census_block_group'].astype(str)
    )

    # Handle missings
    has_missing = (df['census_tract'] == -1) | (df['census_block_group'] == -1)
    df.loc[has_missing, 'bg'] = ''

    # Check that length is right
    l = df.loc[df['bg'] != '', 'bg'].str.len()
    assert l.min() == l.max() and l.max() == 12

    df = df.drop(['census_block_group', 'census_tract', 'mm_fips_muni_code'],
                 axis=1)
    return df

def _clean_geocodes(df):
    # Drop uncoded or badly coded
    # 0 - Street level
    # 1 - zip+4
    # 2 - zip+2
    # 3 - zip
    # 4 - uncoded
    bad_geocode = (df['x_coord'].isnull()) | (df['x_coord'] == 0)
    df = df[~bad_geocode].copy()

    # Correct longitude
    df['x_coord'] *= -1

    # Get UTM
    df['utm_east'], df['utm_north'] = utmz11(df['x_coord'].values,
                                             df['y_coord'].values)

    for utm in UTM:
        df[utm] = np.around(df[utm]).astype(np.int32)

    # Match current code (expects varnames `x` and `y`)
    df = df.rename(columns={'x_coord': 'x', 'y_coord': 'y'})

    return df

def _make_house_grid(df):
    for utm in UTM:
        df[utm + '_real'] = df[utm]
        df[utm] = np.around(df[utm] / HGRID_SIZE) * HGRID_SIZE
        df[utm] = df[utm].astype(np.int32)

    df = group_id(df, cols=UTM, name='hgrid', merge=True)
    return df

def _recast(df):
    new_types = (
        (np.int16, ('year',)),
        (np.int8,
         ('quarter', 'dup_flag', 'corporation_buyer', 'corporation_seller',
          'poss_correction')),
    )
    for dtype, cols in new_types:
        for col in cols:
            if col not in df or df[col].dtype == dtype:
                continue
            else:
                df[col] = df[col].fillna(-1).astype(dtype)

    return df

def _reorder_cols(df):
    priority = ['property_id', 'year', 'quarter', 'p_real',
                'sqft', 'lotsize', 'beds', 'baths', 'rooms', 'yr_blt']
    remainder = [x for x in df.columns if x not in priority]
    df = df[priority + remainder]
    df = df.reset_index(drop=True)
    return df


# Stack (and clean) county files
@load_or_build(dq_src_path('tmp_history_cleaned.pkl'))
def clean_history():
    """
    Append history files and make major sample restrictions.
    """
    df = load_history()

    df['ltv'] = df['origination_loan'] / df['val_transfer']

    df = df[df['poss_correction'] != 1]

    bad = (
        (df['val_transfer'] <= 15000) |
        (df['bad_history_transaction'] != 0) |
        (df['transfer'] == 1) |                     # Not arms length
        (df['group_sale'] == 1) |                   # A group sale
        (df['partial_sale'] == 1) |
        (df['partial_consideration'] == 1) |
        (df['tran_type'] != 1) |                    # Is not a resale
        (df['ltv'] > 2) | (df['ltv'].isnull()) |
        (df['distress_indicator'] != -1) |          # Is a distressed sale
        (df['corporation_buyer'] == 3)              # Buyer is gov't
    )
    df = df[~bad]

    # Handle quick re-sales
    if 'dt' in df.columns:
        del df['dt']
    df = _calc_sale_timediff(df)
    df = df[~(df['dt'] <= pd.Timedelta('90D'))]
    # df = _handle_quick_resales(df)

    # Consolidate to quarter (with cutoff = dt <= 90, this is 1 ob)
    df['year'] = df['date_transfer'].dt.year
    df['quarter'] = df['date_transfer'].dt.quarter
    df = df.drop_duplicates(['property_id', 'year', 'quarter'], keep='last')

    # Deflate
    df = _deflate_prices(df)

    return df

def _okay_dupes(df):
    """
    `dup_flag` = 1: "A transaction (possibly with loan) with one or more
      refi's.
    `dup_flag` = 6: "Multiple Sellers with same buyer." (Often quick refi's or
      corrections)
    Want to keep these unless they're actually duplicates.
    """
    keep_if_dupe_code = (1, 6)
    # Get count of property-day
    prop_date = ['property_id', 'date_transfer']
    tT = df.groupby(prop_date).size()
    df = df.join(tT.to_frame('tT'), on=prop_date)

    df['dupe_okay'] = ((df['tT'] == 1) &
                       (df['dup_flag'].isin(keep_if_dupe_code)))

    return df

def _handle_quick_resales(df):
    # Calc days between sale of same prop
    df = _calc_sale_timediff(df)
    df = _date_cluster_id(df, id_name='cluster_id')

    # Get cluster size
    c_size = df.groupby('cluster_id').size()
    df = df.join(c_size.to_frame('c_size'), on='cluster_id')

    # Drop clusters bigger than 3
    # NOTE: These serial flippers or mis-labeled subdivisions (<.1% of sample)
    df = df[df['c_size'] <= 3]

    # Pull out close sales with one valid loan
    # # Get p diff in prices
    p_min = df.groupby('cluster_id')['val_transfer'].min()
    p_max = df.groupby('cluster_id')['val_transfer'].max()
    p_diff = (p_max - p_min) / p_min
    df = df.join(p_diff.to_frame('p_diff'), on='cluster_id')
    # # Get valid loan flag and group count
    df['valid_loan'] = df['origination_loan'] > 0
    num_valid_loans = df.groupby('cluster_id')['valid_loan'].sum()
    df = df.join(num_valid_loans.to_frame('n_valid_loans'), on='cluster_id')
    # # Drop
    non_valid_to_drop = (               # Drop if
        (df['c_size'] > 1) &            # ...you're in a sale cluster
        (np.abs(df['p_diff']) < .1) &   # ...whose prices are close
        (df['n_valid_loans'] == 1) &    # ...with only one valid loan
        (~df['valid_loan'])             # ...and you're not it
    )
    df = df[~non_valid_to_drop]

    # For everyone else, keep the last one
    df = df.drop_duplicates('cluster_id', keep='last')

    tmp_vars = ['cluster_id', 'c_size', 'p_diff', 'valid_loan',
                'n_valid_loans']
    df = df.drop(tmp_vars, axis=1)

    return df

def _calc_sale_timediff(df):
    df = df.sort_values(['property_id', 'date_transfer', 'val_transfer'],
                        ascending=[True, True, False])
    date_diff = df['date_transfer'].diff()
    not_first = df.duplicated('property_id')
    date_diff.loc[~not_first] = np.nan
    df['dt'] = date_diff
    return df

def _date_cluster_id(df, id_name='cluster_id'):
    cutoff = pd.Timedelta('90D')
    below_cutoff = (df['dt'] <= cutoff)
    max_cluster_size = int(below_cutoff.groupby(df['property_id']).sum().max())
    df[id_name] = np.arange(len(df), dtype=np.int64)
    for i in xrange(max_cluster_size):
        offset = df[id_name].shift().fillna(1).astype(np.int64)
        df.loc[below_cutoff.values, id_name] = offset[below_cutoff.values]

    return df

def _deflate_prices(df):
    cpi = cpi_quarter()
    deflator = 'cpi_to2014'
    df = df.join(cpi[deflator], on=['year', 'quarter'])
    df['p_real'] = df['val_transfer'] * df[deflator]
    df['loan_real'] = df['origination_loan'] * df[deflator]
    del df[deflator]
    return df


@load_or_build(data_path('houses_places.pkl'))
def load_houses_places():
    df = load_assessor()
    df = df.rename(columns={'site_zip': 'zip',
                            'site_city': 'city'})
    df = df[['property_id', 'city', 'zip']].drop_duplicates()
    df = df.sort_values('property_id')
    df = df.reset_index(drop=True)
    return df


def load_houses_utm():
    df = load_house_sample()[UTM].drop_duplicates()
    df.reset_index(inplace=True, drop=True)
    return df


if __name__ == '__main__':
    df = clean_history(_rebuild=True)
    df = load_house_sample(_rebuild=True)
