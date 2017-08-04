from os import path

import pandas as pd

from econtools import stata_merge, force_iterable, load_or_build

from util import UTM
from util.system import data_path
from clean import load_geounit

old_region_by_firm = {1: [4, 7], 2: [3], 3: [1, 8], 4: [2, 6], 5: [10]}

# Outer edge is 9755, 800123, 115394
region_by_firm = {
    1: [18763, 800075],
    2: [14052],
    3: [115314, 800170],
    4: [115394, 800074],
    # 5: [115389],
    6: [9755],      # XXX why is this here?
    7: [800123],    # To grab last chunk of high-variance land to NE
    8: [800335],    # To get (most of) Rolling Hills et al
    # 9: [800144]   # I don't think it gets any more than 800335
}

obs_geounit = {'sale': 'house', 'hgrid': 'house', 'patzip': 'patzip'}

sig_labels = {1: '', .1: '*', .05: '**', .01: '***'}

eol = " \\\\ \n"


# Latex table stuff (move to `econtools`)
def table_statrow(rowname, vals, name_just=24, stat_just=12, wrapnum=False,
                  sd=False,
                  digits=None,
                  lempty=0, rempty=0, empty=[],
                  ):
    """
    `digits` must be specified for numerical values, otherwise assumes string.
    """
    outstr = rowname.ljust(name_just)

    if wrapnum:
        cell = "\\num{{{}}}"
    else:
        cell = "{}"
    if sd:
        cell = "(" + cell + ")"
    cell = "& " + cell

    for i, val in enumerate(vals):

        if i in empty:
            outstr += "& ".ljust(stat_just)
            continue

        if digits is not None:
            printval = _format_nums(val, digits=digits)
        else:
            printval = val
        outstr += cell.format(printval).ljust(stat_just)

    # Add right-hand empty cells if needed
    max_val_index = len(vals) - 1
    if len(empty) > 0 and (max(empty) > max_val_index):
        outstr += "& ".ljust(stat_just)*(max(empty) - max_val_index)

    outstr += eol
    return outstr


def table_mainrow(rowname, varname, regs,
                  lempty=0, rempty=0, empty=[],
                  name_just=24, stat_just=12, digits=3):

    # Translate old `lempty` into `empty` list
    len_vals = len(force_iterable(regs))
    empty, len_row = table_row_parse_empty(empty, lempty, rempty, len_vals)

    # Constants
    se_cell = "& ({})"
    blank_stat = "& ".ljust(stat_just)
    # Build beta and SE rows
    beta_row = rowname.ljust(name_just)
    se_row = " ".ljust(name_just)
    nonempty_col = 0
    for i in range(len_row):
        if i in empty:
            beta_row += blank_stat
            se_row += blank_stat
        else:
            stats = _get_stats(force_iterable(regs)[nonempty_col],
                               varname, '', digits)
            this_beta = "& {}".format(stats['_beta'] + stats['_sig'])
            beta_row += this_beta.ljust(stat_just)
            se_row += se_cell.format(stats['_se']).ljust(stat_just)
            nonempty_col += 1
    assert nonempty_col == len_vals

    full_row = beta_row + eol + se_row + eol

    return full_row

def _get_stats(reg, varname, label, digits=3):
    beta = _format_nums(reg.beta[varname], digits=digits)
    se = _format_nums(reg.se[varname], digits=digits)
    sig = _sig_level(reg.pt[varname])
    names = ['beta', 'sig', 'se']
    stats_dict = dict(zip(
        ['{}_{}'.format(label, x) for x in names],
        (beta, sig, se)
    ))
    return stats_dict

def _format_nums(x, digits=3):
    if type(x) is str:
        return x
    else:
        return '{{:.{}f}}'.format(digits).format(x)

def _sig_level(p):
    if p > .1:
        p_level = 1
    elif .05 < p <= .1:
        p_level = .1
    elif .01 < p <= .05:
        p_level = .05
    else:
        p_level = .01

    return sig_labels[p_level]


def table_row_parse_empty(empty, lempty, rempty, len_vals):
    if (lempty or rempty) and empty:
        raise ValueError
    elif not empty:
        empty = range(lempty) + range(lempty + len_vals, len_vals + rempty)
    len_empty = len(empty)
    len_row = len_empty + len_vals

    return empty, len_row


def join_latex_rows(row1, row2):
    """
    Assumes both end with `eol` and first column is label.
    """
    row1_noend = row1.replace(eol, "")
    row2_guts = row2.split("&", 1)[1:].replace(eol, "")
    joined = row1_noend + row2_guts
    return joined


def write_notes(notes, table_path):
    split_path = path.splitext(table_path)
    notes_path = split_path[0] + '_notes.tex'
    with open(notes_path, 'w') as f:
        f.write(notes)


def _print_reg(name, reg):
    print "\n{}:".format(name)
    with pd.option_context('display.max_rows', None):
        print reg.summary.iloc[:, :4]


# Aux/data methods
@load_or_build(data_path('house_grid_balanced.p'))
def load_hgrid_panel(years=None):
    # TODO: This is a more general cleaning, shouldn't be in regutils

    houses = load_geounit('house')

    # Build basic balanced hgrid panel
    house_utms = houses[UTM].drop_duplicates()
    if not years:
        years = houses['year'].unique()
    hgrid_panel = _balance_panel(house_utms, years)

    # Get grid's block group
    hgrids_bg = _define_hgrids_othergeo(houses, geo='bg')
    hgrid_panel = stata_merge(hgrid_panel, hgrids_bg, on=UTM, how='left',
                              assertval=3)

    # Get back 'hgrid' defined in original house cleaning
    xwalk_hgrid = houses[UTM + ['hgrid']].drop_duplicates()
    hgrid_panel = stata_merge(hgrid_panel, xwalk_hgrid, on=UTM, how='left',
                              assertval=3)

    # Fix UTM dtypes
    hgrid_panel[UTM] = hgrid_panel[UTM].astype(int)

    return hgrid_panel


def _balance_panel(df, years):
    df = df.copy()
    df['key'] = 1
    fill_year = pd.DataFrame(years, columns=['year'])
    fill_year['key'] = 1
    balanced = pd.merge(df, fill_year, on='key')
    del balanced['key']
    return balanced


def _define_hgrids_othergeo(rawhouses, geo='bg'):
    """
    Assigns a house's UTM grid to some other geography (e.g., bg, zip) as the
    geography listed for the majority of houses in the grid.
    """
    # TODO: Refactor this to be more general; use `count_by_cols`?
    # TODO: test me?
    property_list = rawhouses[UTM + ['property_id', geo]].drop_duplicates()
    property_counts = property_list.groupby(UTM + [geo]).size().squeeze()
    property_counts.name = 'counts'
    property_counts = property_counts.reset_index()
    hgrids_geo = property_counts.sort_values(UTM + ['counts']).drop_duplicates(
        UTM, keep='last')
    del hgrids_geo['counts']
    return hgrids_geo


def count_by_cols(df, cols, var, merge=False):
    """Get counts of non-missing `var` in columns ``."""
    # TODO: Move to mysci?
    valid_obs = df.loc[df[var].notnull(), cols]
    count_df = valid_obs.groupby(cols).size()

    # Get groupbycols back as columns for merging
    count_var = 'count_' + var
    count_df.name = count_var
    count_df = count_df.reset_index()

    if merge:
        count_df = pd.merge(df, count_df, on=cols, how='left')
        count_df[count_var].fillna(0, inplace=True, downcast='infer')

    return count_df


def region_def_by_firm(region=None):
    if region in region_by_firm:
        firmlist = region_by_firm[region]
    elif region is None:
        firmlist = set.union(*[set(x) for x in region_by_firm.values()])
    else:
        raise ValueError

    return firmlist


if __name__ == "__main__":
    pass
