from __future__ import division, print_function

import pandas as pd

from util import UTM
from util.system import data_path


# Monitors
def load_monitors(year0=1997, yearT=2005, freq='q', fullcover=True):
    freq_key = {'q': 'quarter', 'a': 'year', 'y': 'year'}
    df = pd.read_stata(data_path('monitor_{}.dta').format(freq_key[freq]))
    # UTM to int
    for utm in UTM:
        df[utm] = df[utm].astype(int)
    # Restrict by year
    yearlist = range(year0, yearT + 1)
    df = df[df['year'].isin(yearlist)]
    # Keep only good nox obs
    timevars = ['year', 'quarter'] if freq == 'q' else ['year']
    if freq == 'q':
        df['quarter'] = df['quarter'].astype(int)

    df = df[UTM + timevars + ['x', 'y', 'site', 'nox', 'ozone']]
    df = df[df['nox'].notnull()]
    # Get obs counts
    if fullcover:
        df = keep_full_coverage_mons(df, year0, yearT, freq=freq)

    return df


def keep_full_coverage_mons(df, year0, yearT, freq='q'):
    obs_per_monitor = df.groupby('site').size()
    df = df.join(obs_per_monitor.to_frame('T'), on='site')
    yearlist = range(year0, yearT + 1)
    full_coverage = len(yearlist)
    if freq == 'q':
        full_coverage *= 4

    df = df[df['T'] == full_coverage]
    del df['T']

    return df


if __name__ == '__main__':
    df = load_monitors()
