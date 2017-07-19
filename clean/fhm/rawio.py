from os import path

import pandas as pd

from util.system import src_path, data_path

FHM_PATH = src_path('fhm')


def raw_firm_data():
    filepath = path.join(FHM_PATH, 'allyrs6.dta')
    return pd.read_stata(filepath)


def electric_flag():
    fhm_elec = pd.read_stata(path.join(FHM_PATH, 'electric.dta'))
    fhm_elec_facids = fhm_elec.loc[fhm_elec['r2009'] == 1, 'facid'].unique()
    fhm_elec_facids = [int(x) for x in fhm_elec_facids]
    as_series = pd.Series(index=fhm_elec_facids, name='electric')
    as_series[:] = 1
    return as_series


def load_firmpanel():
    """Generated by Stata"""
    filepath = data_path('firm_panel.dta')
    return pd.read_stata(filepath)


def load_firmswide(firm_id=None):
    """Generated by Stata"""
    filepath = data_path('firms_static.dta')
    firms_wide = pd.read_stata(filepath)

    if firm_id:
        firms_wide = firms_wide[firms_wide['firm_id'] == firm_id].squeeze()

    if firms_wide.empty:
        raise ValueError("Firm id '{}' not found.".format(firm_id))

    return firms_wide
