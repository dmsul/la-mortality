import pandas as pd

from econtools import load_or_build

from util.system import data_path
from clean.pr2.rawio import build_int_qcer
from clean.pr2.firmgroups import load_firmgroups, group_lists_full, get_grouprep


@load_or_build(data_path('pr2_nox.p'), copydta=True)
def emissions():
    """NOx emissions from `pr2` combined within 'firm group'."""

    emi = build_int_qcer().set_index(['facid', 'year', 'mth'])['emi']
    emi = emi.sort_index()
    emi = correct_emi(emi)
    # replace `facid` with group rep facid, sum within new facid
    emi = emi.reset_index()
    grouplists = group_lists_full()
    emi['facid'] = emi['facid'].apply(lambda x: get_grouprep(grouplists.loc[x]))
    emi['quarter'] = (emi['mth'] / 3).astype(int)

    emi = emi.groupby(['facid', 'year', 'quarter'])['emi'].sum()

    if 1 == 0:  # Browse leftover oddballs
        import ipdb
        groups = load_firmgroups()
        purged_facids = tuple([x[0] for x in manual_changes()])
        for repid, row in groups.iterrows():
            group = row.dropna().astype(int).tolist()
            # Don't stop if there's no "weirdo" flag
            if -1 not in group:
                continue
            # Don't stop if it's been handled manually
            if max([x in purged_facids for x in group]):
                continue

            group = [x for x in group if x != -1]
            groups_emi = emi.loc[group].unstack('facid')
            print groups_emi
            ipdb.set_trace()
            print 'wut'

    return emi


def correct_emi(indf):
    change_these = manual_changes()
    emi = indf.copy()
    for facid, year, month, newval in change_these:
        emi.loc[pd.IndexSlice[facid, year, month]] = newval

    return emi


def manual_changes():
    """Corrections for duplications etc. within firm groups."""
    change_these = [
        # facid, year, mth, newamt
        (800370, 1998, 12, 0),
        (800416, 2003, 12, 0),
        (800208, 2002, 9, 0),  # This one is weird, doesn't match pr3
        (117006, 1998, 12, 0),
        (800126, 1998, 6, 0),  # Unclear, assume smaller val is subset of larger
        (800126, 2003, 12, 0),
        (122295, 2000, 3, 0),
        (800241, 1995, 3, 0),
        (93073,  2003, 12, 0),
        (83753,  2004, 9, 0),  # Definite duplicate per pr3
        (133046, 2003, 6, 0),  # This is the 'wrong' value per pr3
        (123087, 2000, 6, 0),  # Both wrong, see next
        (69677, 2000, 6, 4.09568),  # Both wrong, see above
        (54183, 1994, 9, 0),
        (54183, 1994, 12, 0),
        (54183, 1995, 3, 0),
        (131249, 2002, 9, 0),           # These guys are a total mess
        (131249, 2002, 12, 0),
        (131249, 2003, 3, 67.5598),
        (131249, 2003, 9, 94.009587),
        (44551, 2001, 12, 0),
        (138568, 2003, 12, 0),
        (126050, 2001, 3, 0),
        (129497, 2004, 12, 2.220865),  # This guy mess, prob not really in group
        (123774, 2000, 3, 0),           # Another mess (pr3 is lower than pr2!)
        (123774, 2000, 9, 0),
        (123774, 2000, 12, 0),
        (112164, 1997, 3, 0),  # Judge call: drop this, result is betw t+1, t-1
        (112164, 2004, 9, 0),
        (141012, 2004, 12, 0),
        # (115536, 1998, 6, wtf) # Judge, unclear, just add
        # (73899) # Judge, unclear, just add
        (119920, 1999, 3, 0),
        (119920, 1999, 6, 0),
        (119920, 1999, 12, 0),
        (6505, 1999, 3, 0),
        (6505, 1999, 6, 0),
        (117247, 1998, 12, 0),
    ]
    return change_these


if __name__ == "__main__":
    df = emissions()
