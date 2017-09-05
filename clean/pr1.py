import glob

import pandas as pd
import numpy as np

from econtools import load_or_build

from util.system import data_path, src_path
from clean.fhm import load_firmpanel


@load_or_build(data_path('pr1_raw.p'), copydta=True)
def load_pr1_raw():

    pr1_pattern = src_path('scaqmd', 'pr1', '*.xlsx')
    the_files = glob.glob(pr1_pattern)
    df = pd.DataFrame()
    for f in the_files:
        tmp = pd.read_excel(f).rename(columns=lambda x: x.lower())
        df = df.append(tmp)
        del tmp
    if df.empty:
        raise ValueError('No excel files read!')

    # Get dates
    df['year'] = df['report_date'].apply(lambda x: x.year)
    df['month'] = df['report_date'].apply(lambda x: x.month)
    df['day'] = df['report_date'].apply(lambda x: x.day)
    df['quarter'] = np.floor((df['month'] - 1) / 3) + 1
    del df['report_date']  # pd.datetime doesn't play nice with pd.to_stata

    return df


def clean_pr1_raw(freq='q', cov_freq='q'):
    df = load_pr1_raw()

    # Restrict years
    df = df[df['year'] <= 2007]

    # Convert to tons
    df['noxt'] = df['amount'] / 2000.

    # Set time unit
    base_col_vars = ['facility_id', 'name', 'year']
    col_vars = {
        'y': base_col_vars,
        'a': base_col_vars,
        'q': base_col_vars + ['quarter'],
        'm': base_col_vars + ['quarter', 'month'],
        'd': base_col_vars + ['quarter', 'month', 'day']
    }

    # Quality control
    daily = df.groupby(col_vars['d'])['noxt'].sum().reset_index()

    coverage_cutoff = .9
    coverage_col_vars = col_vars['y']
    coverage = daily.groupby(coverage_col_vars).size() / 365
    coverage = pd.DataFrame(coverage, columns=['coverage']).reset_index()
    coverage['is_good_' + 'y'] = coverage['coverage'] >= coverage_cutoff
    coverage['has_good_' + 'y'] = coverage.groupby(
        'facility_id')['is_good_y'].transform(lambda x: x.max())
    coverage['min_coverage'] = coverage.groupby(
        'facility_id')['coverage'].transform(lambda x: x.min())

    collapsed = df.groupby(col_vars[freq])['noxt'].sum().reset_index()
    collapsed_w_good = pd.merge(collapsed, coverage, on=coverage_col_vars)
    assert collapsed.shape[0] == collapsed_w_good.shape[0]

    return collapsed_w_good


def get_ufacid(df):
    old_firm_panel = load_firmpanel()
    pull_over_vars = ['facid', 'ufacid', 'fname', 'year']
    new = pd.merge(df, old_firm_panel[pull_over_vars].drop_duplicates(),
                   left_on=['facility_id', 'year'], right_on=['facid', 'year'],
                   how='left')
    return new


def sample_select(df):
    return df[(df['year'] <= 2007) & (df['ufacid'].notnull())].copy()


def sanity_checks(df):
    # facility_id and ufacid are unique map
    pairs = df[['facility_id', 'ufacid']].drop_duplicates()
    N = pairs.shape[0]
    assert pairs['facility_id'].unique().shape[0] == N
    assert pairs['ufacid'].unique().shape[0] == N


def coverage_table(df):
    s1 = df[['facility_id', 'year', 'coverage']]
    s1 = s1.set_index(['facility_id', 'year'])['coverage']
    s1 = s1.apply(lambda x: round(x*100, 0))
    df2 = s1.unstack('year')
    return df2


def naive_full():
    pr1 = clean_pr1_raw()
    pr1_2 = get_ufacid(pr1)
    pr1_3 = sample_select(pr1_2)
    sanity_checks(pr1_3)

    pr1_panel_path = data_path('pr1_panel.p')
    pr1_3.to_pickle(pr1_panel_path)
    pr1_3.to_stata(pr1_panel_path.replace('.p', '.dta'), write_index=False)


def only_good_firms(freq='q'):
    # Eyeball check for firms legit through 2005 (use coverage_table)
    # 4477, 25638, 47781, 68042, 115389, 115394, 800074, 800075
    # This is roughly "only has one year below 85"
    df = clean_pr1_raw('q')

    eyeballed_good = (4477, 25638, 47781, 68042, 115389, 115394, 800074, 800075)
    df = df[df['facility_id'].isin(eyeballed_good)]
    df = df[df['year'] <= 2005]

    df.to_stata('../data/pr1_clean_q.dta', write_index=False)
    df.to_pickle('../data/pr1_clean_q.p')


if __name__ == '__main__':
    only_good_firms()
