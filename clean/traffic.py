"""
Clean's July(?) readings for Bay Area traffic monitor stations.
"""
import pandas as pd

from econtools import load_or_build

from util.system import src_path, data_path


@load_or_build(data_path('traffic.p'))
def load_all_days():

    years = range(1999, 2005 + 1)
    months = ('07', '12')
    df = pd.DataFrame()
    for y in years:
        for m in months:
            this_df = read_raw_station_day(y, m)
            if df.empty:
                df = this_df.copy()
            else:
                df = df.append(this_df)

    return df


def read_raw_station_day(year, month):

    file_stem = src_path('traffic', 'd11_text_station_day_{}_{}.txt')
    df = pd.read_csv(file_stem.format(year, month),
                     names=['date', 'station_id', 'region', 'route', 'dir',
                            'lanetype', 'station_length', 'samples',
                            'perc_observed', 'total_flow', 'delay_35',
                            'delay_40', 'delay_45', 'delay_50', 'delay_55',
                            'delay_60']
                     )

    # Clean date
    date = df['date'].str.split(' ').apply(lambda x: pd.Series(x[0].split('/')))
    date = date.astype(int)
    date.columns = ['month', 'day', 'year']
    del df['date']
    df = df.join(date)

    return df


if __name__ == '__main__':
    df = load_all_days()
