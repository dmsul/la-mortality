"""
Convert DataQuick DTA's to PKL.

Only sample restrictions is `use_code_std = 1` (single family homes) in history
data.
"""
from __future__ import division, print_function

import os

import numpy as np
import pandas as pd

from econtools import load_or_build


YEARS = range(1993, 2008)  # Allow some buffer
COUNTIES = ('los angeles', 'orange', 'riverside', 'san bernardino')


# Path methods
def sq_history_dta_path(county_name, year=None):
    if year:
        file_str = 'History-CA-{}_{}.dta'.format(county_name, year)
    else:
        file_str = 'History-CA-{}.dta'.format(county_name)
    return dq_dta_path('History-CA', file_str)

def sq_dta_assessor_path(county_name):
    file_str = 'Assessor-CA-{}.dta'.format(county_name)
    return dq_dta_path('Assessor-CA', file_str)

def dq_dta_path(*args):
    """ Path to original DataQuick DTA files """
    els = ('d:\\data', 'DataQuick')
    path = os.path.join(*(els + args))
    return path

def dq_src_path(*args):
    """ Path to Python-friendly binary data files """
    return dq_dta_path('pkl', *args)


# Assessor
@load_or_build(dq_src_path('tmp_assessor_appeneded.pkl'))
def load_assessor():
    df = _county_appender('assessor')
    return df


@load_or_build(dq_src_path('{}-assessor.pkl'), path_args=[0])
def load_assessor_county(county_name):
    # Load Raw data
    filepath = sq_dta_assessor_path(county_name)
    keep_vars = _assessor_keep_vars()
    if county_name == 'los angeles':
        # This one is too big to read at once
        # (numpy 1.10.4, np.frombuffer throws "can't convert to C long")
        reader = pd.read_stata(filepath, chunksize=100000,
                               convert_categoricals=False)
        df = pd.concat([_assessor_recast(df[keep_vars]) for df in reader],
                       ignore_index=True)
    else:
        df = pd.read_stata(filepath, convert_categoricals=False)
        df = _assessor_recast(df[keep_vars])

    df = _assessor_rename(df)

    return df

def _assessor_keep_vars():
    keep_vars = [
        'sa_property_id',
        'sa_parcel_nbr_primary',
        'sa_site_house_nbr',            # Address
        'sa_site_dir',
        'sa_site_street_name',
        'sa_site_suf',
        'sa_site_city',
        'sa_site_zip',
        'sa_site_unit_val',
        # 'mm_muni_name',
        'mm_fips_muni_code',
        'sa_sqft',                        # Characteristics
        'sa_nbr_rms',
        'sa_nbr_bath',
        'sa_nbr_bedrms',
        'sa_nbr_stories',
        'sa_lotsize',
        'sa_bldg_sqft',
        'sa_yr_blt',
        'assr_year',
        'sa_val_assd',
        'sa_val_assd_land',
        'sa_val_assd_imprv',
        'sa_construction_qlty',
        'sa_cool_code',
        'sa_heat_code',
        'sa_roof_code',
        'sa_structure_nbr',
        'sa_yr_blt_effect',
        'use_code_std',
        'sa_x_coord',                   # Geocodes
        'sa_y_coord',
        'sa_geo_qlty_code',
        'sa_census_tract',
        'sa_census_block_group',
        'bad_address',                  # Flags added by cleaners
        'bad_assessor',
    ]
    return keep_vars

def _assessor_recast(df):
    new_types = (
        (np.int32, None, ('sa_property_id',)),
        (np.int64, -1, ('sa_parcel_nbr_primary',)),
        (np.float32, None, ('sa_sqft', 'sa_nbr_rms', 'sa_nbr_bath',
                            'sa_nbr_bedrms', 'sa_nbr_stories', 'sa_lotsize',
                            'sa_bldg_sqft', 'sa_yr_blt', 'sa_yr_blt_effect',
                            'sa_val_assd', 'sa_val_assd_land',
                            'sa_val_assd_imprv',)),
        (np.int8, -1, ('sa_construction_qlty', 'sa_cool_code', 'sa_heat_code',
                       'sa_roof_code', 'sa_structure_nbr', 'bad_assessor',
                       'use_code_std', 'sa_geo_qlty_code',
                       'sa_census_block_group')),
        (np.int32, -1, ('sa_census_tract',)),
    )
    for dtype, fillna, cols in new_types:
        for col in cols:
            if col not in df or df[col].dtype == dtype:
                continue
            elif fillna is not None:
                df[col] = df[col].fillna(fillna).astype(dtype)
            else:
                df[col] = df[col].astype(dtype)
    return df

def _assessor_rename(df):
    df = df.rename(columns=lambda x: x.replace('sa_', ''))
    rename = {
        'parcel_nbr_primary': 'parcel_nbr',
        'nbr_rms': 'rooms',
        'nbr_bath': 'baths',
        'nbr_bedrms': 'beds',
        'nbr_stories': 'stories',
    }
    df = df.rename(columns=rename)
    return df


# History (sales)
@load_or_build(dq_src_path('tmp_history_appended.pkl'))
def load_history():
    df = _county_appender('history')
    return df


@load_or_build(dq_src_path('{}-history.pkl'), path_args=[0])
def load_history_county(county_name):

    dfs = [load_history_year(county_name, year=year) for year in YEARS]
    df = pd.concat(dfs, ignore_index=True)
    del dfs
    return df

@load_or_build(dq_src_path('{}-history-{}.pkl'), path_args=[0, 'year'])
def load_history_year(county_name, year=None):

    df = load_history_raw(county_name, year=year)
    # Restrict variables
    keep_vars = _history_keep_vars()
    df = df[keep_vars]
    # Restrict sample (too big otherwise)
    df = df[df['use_code_std'] == 1]
    assert len(df) > 0
    del df['use_code_std']

    df = _history_recast(df)
    df = _history_rename(df)

    return df

def _history_keep_vars():
    keep_vars = [
        'sr_property_id',
        'sr_unique_id',
        'sr_date_transfer',
        'sr_date_filing',
        'use_code_std',                 # Use code
        'sr_val_transfer',              # Price and Transaction
        'transfer',
        'sr_tran_type',
        'distress_indicator',
        'corporation_buyer',
        'corporation_seller',
        'sr_buyer',                     # Buyer-Seller
        'sr_seller',
        'origination_loan',             # Loan info
        'estimated_interest_rate_1',
        'bad_history_transaction',      # NOTE: see `close_repeat_sale`
        'partial_consideration',
        'partial_sale',
        'group_sale',
        'dup_flag',                     # Flags added by cleaners
        'close_repeat_sale',            # NOTE: Broken/randomly assigned for
                                        #       same-day obs
        'poss_correction',
    ]
    return keep_vars

def _history_recast(df):
    # dtype, fillna, cols
    new_types = (
        (np.float32, None, ('origination_loan', 'estimated_interest_rate_1')),
        (np.int32, None, ('sr_property_id', )),
        (np.int8, -1, ('transfer', 'bad_history_transaction',
                       'sr_tran_type', 'partial_sale', 'distress_indicator',
                       'corporation_buyer', 'corporation_seller',
                       'dup_flag', 'close_repeat_sale',
                       'poss_correction', 'group_sale', 'partial_sale',
                       'partial_consideration')),
    )
    for dtype, fillna, cols in new_types:
        for col in cols:
            if col not in df or df[col].dtype == dtype:
                continue
            elif fillna is not None:
                df[col] = df[col].fillna(fillna).astype(dtype)
            else:
                df[col] = df[col].astype(dtype)

    return df

def _history_rename(df):
    df = df.rename(columns=lambda x: x.replace('sr_', ''))

    return df


@load_or_build(dq_src_path('raw', '{}_{}.pkl'), path_args=[0, 'year'])
def load_history_raw(county_name, year=None):

    df = pd.read_stata(sq_history_dta_path(county_name, year=year),
                       convert_categoricals=False,
                       )

    return df


def _county_appender(dataset):
    if dataset == 'history':
        func = load_history_county
    elif dataset == 'assessor':
        func = load_assessor_county
    else:
        ValueError

    dfs = [func(c) for c in COUNTIES]
    df = pd.concat(dfs, ignore_index=True)
    del dfs
    return df


if __name__ == '__main__':
    if 1:
        import gc
        for c in COUNTIES:
            for y in YEARS:
                df = load_history_year(c, year=y, _rebuild=True)
                del df
            df = load_history_county(c, _rebuild=True)
            del df
            gc.collect()
