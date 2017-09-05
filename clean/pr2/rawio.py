import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

from econtools import load_or_build

from util.system import src_path, data_path


def build_int_qcer():
    """Basic emissions panel"""
    qcer_emi = raw_int_qcer()
    qcer_emi['entry_date'] = pd.to_datetime(qcer_emi['entry_date'],
                                            errors='coerce')

    qcer_emi = qcer_emi[qcer_emi['dev_cat'] == 'TOTAL'].drop('dev_cat', axis=1)
    qcer_emi = qcer_emi[qcer_emi['emi_id'] == 'NOX'].drop('emi_id', axis=1)

    unique_idx = ['facid', 'year', 'mth']

    # Take the most recent/updated value
    qcer_emi = qcer_emi.sort_values(unique_idx + ['entry_date'])
    unique = qcer_emi.drop_duplicates(unique_idx, keep='last')

    # Handle problem facids
    unique = _handle_problem_facids(unique)

    return unique

def _handle_problem_facids(df):         #noqa
    drop_these = [
        114161,  # No address, can't match it, one q of small emi
        142188,  # No address, can't match it, one q of small emi, offshore
        800023,  # No address, can match, but looks like dup of 101843
    ]
    df = df[~df['facid'].isin(drop_these)]

    return df


# Separate from main 'built' method for tests below
@load_or_build(data_path('pr2_raw_int_qcer.p'))
def raw_int_qcer():
    """Emissions table doesn't have `facid`, get it from `int_rpt`"""
    int_qcer_emi = read_pr2table('int_qcer_emi')
    xwalk = read_pr2table('int_rpt')

    qcer_emi = pd.merge(int_qcer_emi, xwalk, on='rpt_key', how='inner',
                        suffixes=('', '_xwalk'))
    qcer_emi['emi'] /= 2000

    return qcer_emi


# Read raw tables
def read_pr2table(table, clean=False):

    # Table-specific `read_csv` args
    rpt_key_tables = ('int_rpt', 'int_qcer', 'int_qcer_emi', 'int_audit',
                      'int_audit_emi', 'AUDIT_DATA')
    csv_kwargs = dict()
    if table == 'int_rpt':
        csv_kwargs['skiprows'] = [1]
    if table in rpt_key_tables:
        csv_kwargs['dtype'] = {'rpt_key': str}

    filepath = src_path('scaqmd', 'pr2', '{}.csv')
    df = pd.read_csv(filepath.format(table), **csv_kwargs)
    df = _strip_string_cols(df)
    df = df.rename(columns=_rename_pr2())

    # More advanced cleaning
    if clean:
        if table == 'rtc_address':
            # Note: I checked these, nothing lost [2/26/15]
            df = df.drop('street_apt', axis=1)
            df = _use_only_location(df)
            df = df.set_index('facid')
            df = df.applymap(_to_string)
            df = df.apply(_std_adds_variables, axis=1)
            df = _missing_address(df)

        # XXX This flag to restrict to emitting firms used to be universal,
        #       Can't remember what it was for. Keep an eye out.
        if table in ('rtc_address',):
            emit_facids = tuple(build_int_qcer()['facid'].unique())
            df = df[df.index.isin(emit_facids)]

    return df

def _strip_string_cols(df):  #noqa
    string_cols = df.dtypes[df.dtypes == 'object'].index
    for col in string_cols:
        df[col] = df[col].str.strip()

    return df

def _rename_pr2():  #noqa
    rename = {
        'fac_id':       'facid',
        'yr':           'year',
        'bus_info_id':  'facid'}
    return rename

def _to_string(cell):         #noqa
    if isinstance(cell, str):
        return cell
    elif np.isnan(cell):
        return ''
    else:
        return str(cell)

# For 'rtc_address'
def _use_only_location(df):     #noqa
    """For use with 'rtc_address'"""
    # Check that every `facid` has a location
    has_loc = df.groupby('facid').apply(
        lambda x: 'LOC' in x['system_type'].tolist())
    has_loc.name = 'has_loc'
    df = df.join(has_loc, on='facid')
    no_loc = df[~df['has_loc']]
    # The 700000 block of `facid`s is people buying permits to take them
    # off the market. They don't emit.
    assert (no_loc['system_type'] == 'TRADING').min()
    df = df[df['has_loc']]
    df = df.drop('has_loc', axis=1)

    # Use location address
    df = df[df['system_type'] == 'LOC'].drop('system_type', axis=1)

    return df

def _std_adds_variables(x):     #noqa
    street = ' '.join(x['street_nbr':'street_sfx'])
    zipcode = '-'.join(x[['zip', 'zip_four']])
    add_vars = pd.Series([street, x['city'], x['state'], zipcode],
                         index=['street', 'city', 'state', 'zip'],
                         name=x.name)
    return add_vars

def _missing_address(df):           #noqa
    """These firms are completely missing from the address file."""
    no_adds = {
        127380: 132192,  # No address, but match online address and emit ts
        127381: 132191   # No address, but match online address and emit ts
    }
    address_vars = ['street', 'city', 'state', 'zip']
    for bad, good in no_adds.iteritems():
        df.loc[bad, :] = df.loc[good, :]

    return df


# TODO: Put these in another file?
# TESTS to compare various metrics in PR2 data, show redundancies
def compare_audit_data():
    """
    Show that table 'AUDIT_DATA' is built from other tables in data.
    """

    src_audit_data = read_pr2table('AUDIT_DATA')
    int_audit = read_pr2table('int_audit')
    int_audit_emi = read_pr2table('int_audit_emi')
    int_rpt = read_pr2table('int_rpt')

    my_audit_data = pd.merge(
        int_audit, int_audit_emi, on='rpt_key', how='inner')

    if 1 == 0:
        my_audit_data = pd.merge(my_audit_data,
                                 src_audit_data[['rpt_key', 'facid']],
                                 on='rpt_key', how='left')
    else:
        wut = int_rpt[['rpt_key', 'facid']]
        my_audit_data = pd.merge(my_audit_data, wut, on='rpt_key', how='left')

    my_audit_data.rename(columns={'audit_year': 'year'}, inplace=True)
    my_audit_data = my_audit_data.drop_duplicates().reset_index(drop=True)

    src_audit_data.reset_index(drop=True, inplace=True)

    dual_cols = ['rpt_key', 'facid', 'emi', 'year', 'audit_qtr', 'emi_id']

    expected, result = src_audit_data[dual_cols], my_audit_data[dual_cols]

    assert_frame_equal(expected, result)


def compare_emi():
    """
    Show that 'int_qcer_emi' and 'max_qcer' tables are redundant.
    """

    qcer = build_int_qcer()
    max_q = read_pr2table('max_qcer')
    max_q['emi'] /= 2000

    i, m = _unify_emi(qcer), _unify_emi(max_q)

    assert_frame_equal(i, m)

def _unify_emi(df):  #noqa
    dual_cols = ['emi']
    idx_cols = ['facid', 'year', 'mth', 'emi_id']
    df = df[idx_cols + dual_cols].drop_duplicates()
    df = df.set_index(idx_cols).sort_index()
    return df


def compare_qcer():
    """
    Show that 'rtc_qcer' and other 'qcer_emi' tables (see `compare_emi`) are
    redundant.
    """

    qcer = build_int_qcer(False)

    rtc_qcer = read_pr2table('rtc_qcer')
    rtc_qcer['entry_date'] = pd.to_datetime(rtc_qcer['entry_date'],
                                            coerce=True)
    rtc_qcer['emi'] /= 2000

    i, r = _unify_qcer(qcer), _unify_qcer(rtc_qcer)
    assert_frame_equal(i, r)

def _unify_qcer(df):  #noqa
    dual_cols = ['emi']
    idx_cols = ['facid', 'year', 'mth', 'emi_id', 'entry_date']
    df = df[idx_cols + dual_cols].drop_duplicates()
    df = df.set_index(idx_cols).sort_index()
    return df
