import pandas as pd
import matplotlib.pyplot as plt

from econtools import load_or_build

from util.system import data_path, src_path
from clean.pr2 import elec_facids
from clean.pr2.firmgroups import group_lists_full, get_grouprep


@load_or_build(data_path('pr3_toxics.pkl'))
def emit_toxics(_rebuild_down=False):

    df = raw_toxics(_rebuild=_rebuild_down)
    # Combine within
    groups = group_lists_full()
    df['gfacid'] = df['facid'].apply(_get_groupid, args=(groups,))
    df = df[df['gfacid'] != -1]
    df['facid'] = df['gfacid']
    df = df.drop('gfacid', axis=1)
    df = df.groupby(['facid', 'year']).sum()

    return df

def _get_groupid(x, groups):
    if x in groups:
        return get_grouprep(groups.loc[x])
    else:
        return -1


@load_or_build(data_path('pr3_toxics_raw.p'))
def raw_toxics():

    srcfile = src_path('scaqmd', 'pr3', 'facility_toxics.xls')
    df = pd.read_excel(srcfile, skiprows=3)
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df.rename(columns=lambda x: x.replace('.', '').strip().lower(),
              inplace=True)
    df.rename(columns={'facility id': 'facid'}, inplace=True)

    return df


if __name__ == '__main__':
    chems = emit_toxics()
    ax_dict = dict()
    fig_dict = dict()
    for chem in ('nox', 'sox', 'tsp', 'rog', 'co'):
        fig_dict[chem], ax_dict[chem] = plt.subplots()
        nox = chems[[chem]]
        max_nox = nox.groupby(level='facid').max().squeeze()
        nox = nox.join(max_nox.to_frame('chem_max'))
        nox['schem'] = nox.eval('{} / chem_max'.format(chem))
        # Get elec flag
        nox = nox.reset_index('year').join(elec_facids().to_frame('elec'))
        nox.set_index('year', append=True, inplace=True)
        nox['elec'].fillna(0, inplace=True)

        annual = nox.reset_index().groupby(['elec', 'year'])['schem'].mean()
        el1 = annual.loc[1]
        el0 = annual.loc[0]
        ax_dict[chem].plot(el1.index, el1, 'ro-')
        ax_dict[chem].plot(el0.index, el0, 'bo-')
        ax_dict[chem].set_title(chem)
    plt.show()
