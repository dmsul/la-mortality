from __future__ import division, print_function

import pandas as pd

from util.system import src_path


def cpi_quarter():
    df = pd.read_stata(src_path('cpi_quarterly.dta'))
    df = df.set_index(['year', 'quarter'])
    return df

def cpi_year():
    df = pd.read_stata(src_path('cpi_annual.dta'))
    df = df.set_index('year')
    return df


if __name__ == '__main__':
    pass
