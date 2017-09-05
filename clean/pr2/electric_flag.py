import pandas as pd

from clean.fhm import electric_flag
from clean.pr1 import load_pr1_raw
from clean.pr2.firmgroups import group_lists_full, get_grouprep


def elec_facids(dataset='fhm'):
    """
    Return Series with group reps of firms named as 'electric' in `dataset`.
    """

    if dataset == 'fhm':
        raw_facids = electric_flag().index
    elif dataset == 'pr1':
        raw_facids = _pr1_elecs()
    else:
        raw_facids = electric_flag().index
        raw_facids = raw_facids.union(pd.Index((_pr1_elecs())))

    facids_groups = group_lists_full().loc[raw_facids]
    electric_groupreps = facids_groups.apply(lambda x: get_grouprep(x)).values
    facids_series = pd.Series(index=electric_groupreps, name='electric')
    facids_series.index.name = 'facid'
    facids_series[:] = 1

    return facids_series

def _pr1_elecs():   #noqa
    pr1 = load_pr1_raw()
    pr1_facids = pr1['facility_id'].unique()
    return pr1_facids


if __name__ == '__main__':
    elecs = elec_facids()
