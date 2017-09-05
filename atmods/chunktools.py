from __future__ import division

import numpy as np

from econtools import load_or_build_direct

from util import UTM
from util.system import data_path
from util.distance import getdist
from clean import load_geounit
from clean.pr2 import load_stacks, FirmIDXwalk
from atmods.env import (MAXDIST, ALTMAXDIST, FIRMS_FOR_ALTMAXDIST,
                        JOB_LIMIT_MIN, CPU_SEC_PER_UNIT)


def calc_resources(units, cli_facid_list=None, altmaxdist=False):
    """
    Return frame with unique `facid` index, a `firm_id` column, and sbatch
    job info:
        num_stacks, units, cpu_per_stack, total_cpu, num_chunks, firm_id
    """

    # In case `units` is str, not DF
    if isinstance(units, str):
        units = calc_aermod_units(units, cli_facid_list=cli_facid_list,
                                  altmaxdist=altmaxdist)
    elif cli_facid_list is not None:
        units = units[cli_facid_list].copy()
        assert ~units.isnull().min()

    # Count firms' stacks
    stack_df = load_stacks().set_index('facid')
    stack_count = stack_df.groupby(level='facid').size()

    # # Prep DataFrame for `batchrun`, etc.
    # NOTE: List of `facid` restricted by `units` via `calc_aermod_units`
    df = stack_count.to_frame('num_stacks').join(
        units.to_frame('units'), how='inner')

    df['cpu_per_stack'] = df['units'] * CPU_SEC_PER_UNIT / 60
    # scale up by number of stacks
    df['total_cpu'] = df['cpu_per_stack'] * df['num_stacks']
    # Number of separate jobs to be under limit
    raw_chunk_max = np.ceil(df['total_cpu'] / JOB_LIMIT_MIN)
    # Limit number of chunks to number of stacks
    df['num_chunks'] = np.minimum(raw_chunk_max,
                                  df['num_stacks']).astype(int)

    # Add `firm_id` to `resources` frame
    idxwalk = FirmIDXwalk()
    df['firm_id'] = [idxwalk.get_firmid(fid, group_rep=True)
                     for fid in df.index]

    # Drop stacks with no receptors in range
    df = df[df['cpu_per_stack'] > 0]

    return df


def calc_aermod_units(geounit, cli_facid_list=None, altmaxdist=False,
                      _load=True, _rebuild=False):
    """
    Return Series with unique index `facid`, count of receptors (e.g., house
    grids) within `maxdist` of each firm.
    """

    # If `FIRMS_FOR_ALTMAXDIST` is altered, cache on disk will be off, so just
    # avoid cache on disk altogether
    if altmaxdist:
        _load = False

    # Separate `load_or_build` to accomodate `cli_facid_list`
    if _load and cli_facid_list is None:
        filepath = data_path(
            'tmp_{}_aermod_units_{}.p').format(geounit, altmaxdist)
        df = load_or_build_direct(filepath,
                                  force=_rebuild,
                                  build=calc_aermod_units,
                                  bargs=(geounit,),
                                  bkwargs=dict(altmaxdist=altmaxdist,
                                               _load=False)
                                  )

        return df
    elif _load and cli_facid_list is not None:
        df = calc_aermod_units(geounit, altmaxdist=altmaxdist)
        restricted_df = df[cli_facid_list]
        assert ~restricted_df.isnull().min()
        return restricted_df

    if altmaxdist:
        real_firm_list = _check_firm_list(cli_facid_list, FIRMS_FOR_ALTMAXDIST)
        maxdist = ALTMAXDIST
    else:
        real_firm_list = cli_facid_list
        maxdist = MAXDIST

    firm_utm = load_stacks(real_firm_list).groupby('facid')[UTM].mean()
    geounit_df = load_geounit(geounit)
    is_close = getdist(geounit_df[UTM].drop_duplicates(), firm_utm,
                       within=maxdist)
    aermod_units = is_close.sum()

    return aermod_units

def _check_firm_list(facid_list, env_list):
    """
    Make sure firms from CLI (`facid_list') are in firms flagged for
    `altmaxdist`
    """
    if not facid_list:
        return env_list

    cli_set = set(facid_list)
    env_set = set(env_list)
    if cli_set > env_set:
        raise ValueError("Passed firm list conflicts with `altmaxdist`")
    else:
        return facid_list


if __name__ == '__main__':
    import sys
    geounit = sys.argv[-1]
    df = calc_aermod_units(geounit)
