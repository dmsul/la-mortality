import numpy as np

from econtools import read, load_or_build

from util.system import data_path


def get_cleand_path(name):
    cleand_path = {
        'house':        'house_sample.dta',
        'monitor':      'monitor_quarter.dta',
        'patzip':       'patient_sample.dta',
    }
    return data_path(cleand_path[name])


def load_cleand(name):
    """ OBSOLETE: Only for access to old Stata DTA """

    fname = get_cleand_path(name)

    # Prioritize 'pickle' version
    if name == 'house':
        df = _houses_pickle()
    else:
        df = read(fname)

    return df


@load_or_build(data_path(get_cleand_path('house')).replace('.dta', '.p'))
def _houses_pickle():
    """ Read DTA output by Stata DO-file, drop some vars, save as pickle """
    df = read(get_cleand_path('house'))

    drop_these = ['bad_address', 'dup_flag', 'unique_id', 'geo_qlty_code',
                  'roof_code', 'corporation_buyer', 'corporation_seller',
                  'assr_year']
    for col in drop_these:
        df.drop(col, axis=1, inplace=True)

    recast_float32 = ['lotsize', 'baths', 'beds', 'rooms', 'stories', 'sqft',
                      'quarter', 'year']
    for col in recast_float32:
        df[col] = df[col].astype(np.float32)

    return df
