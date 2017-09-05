from __future__ import division

import pandas as pd

from econtools import force_list, stata_merge


def swap_index(left, right, old_idx, new_idx):
    """ Merge to replace an index """
    # XXX move to `econtools`? What does this even do?
    left_idx = pd.DataFrame([force_list(x) for x in left.index.values],
                            columns=left.index.names)
    reidxed_right = right.reset_index().set_index(old_idx).reindex(
        left_idx.set_index(old_idx).index)
    left_idx.drop(old_idx, axis=1, inplace=True)
    for col in force_list(new_idx):
        left_idx[col] = reidxed_right[col].values
    left.index = pd.MultiIndex.from_tuples(
        [tuple(x) for x in left_idx.values.tolist()],
        names=left_idx.columns
    )
    return left


def build_avg(df, weight, unit_name, subunit_name, weight_name=None,
              oth_index=None,
              force_coverage=False):
    """
    Args:
        df (DF):        Data to be built up from `subunit_name` to `unit_name`
        weight (DF):    `subunit` to `unit` weights and cross-walk
        unit_name (str): Column name of unit
        subunit_name (str): Column name of subunit
    Kwargs:
        weight_name (str):  Name of weighting variable in case `weight` has more
            than three columns
        oth_index (list):    Additional variables to use as ID (e.g., year)

    Returns:
        A weighted average of `df` with index `unit_name` and columns
        `df.columns`.

    Raises:
        ValueError: A (sub)unit name was not in one of the DF's
    """
    # Index names
    subunit_index = force_list(subunit_name)
    unit_index = force_list(unit_name)
    if oth_index:
        oth = force_list(oth_index)
        subunit_index += oth
        unit_index += oth

    # Prep weight matrix
    df_subunit_index = df.reset_index()[subunit_index].copy()
    # Make sure col names are not MultiIndex
    df_subunit_index.columns = subunit_index
    weight = _format_weight(df_subunit_index, weight, unit_name, subunit_name,
                            weight_name,
                            force_coverage=force_coverage)
    weight = weight.reset_index().set_index(subunit_index)
    df_prep_wt = df.reset_index().set_index(subunit_index).mul(
        weight[weight_name], axis=0)
    df_prep_wt.index = pd.MultiIndex.from_tuples(
        [tuple(i) for i in weight.reset_index()[unit_index].values.tolist()],
        names=unit_index,
    )
    if len(unit_index) > 1:
        weighted_avg = df_prep_wt.groupby(level=unit_index).sum()
    else:
        weighted_avg = df_prep_wt.groupby(level=unit_name).sum()

    return weighted_avg

def _format_weight(df_index, weight, unit_name, subunit_name, weight_name,      #noqa
                   force_coverage=False):
    """
    Return weight matrix with same subunit index as `df`, and
    columns `unit_name`, `weight_name`.

    NOTE: Assumes full coverage in `df_index` of `unit_name`: If one subunit of
    a unit is present in `df_index`, all should be present.
    """
    # Check for variables
    new_weight = weight.reset_index()
    for name in (unit_name, subunit_name, weight_name):
        try:
            name in new_weight.columns
        except AssertionError:
            raise ValueError("`{}` not in weighting DataFrame.".format(name))

    # Set index and columns
    new_weight.set_index(subunit_name, inplace=True)
    cols = force_list(unit_name) + force_list(weight_name)
    all_names = cols + force_list(subunit_name)
    new_weight = new_weight[cols].copy()
    # Norm weights to be 1
    wtf = new_weight.reset_index()[all_names]
    unit_total = wtf.groupby(unit_name)[weight_name].sum()
    wtf = wtf.join(unit_total.to_frame('_tot'), on=unit_name)
    wtf[weight_name] /= wtf['_tot']
    normed_weight = wtf.drop('_tot', axis=1)
    # Add `oth_index` and/or re-index to master dataframe via 'right' join
    normed_weight = stata_merge(df_index, normed_weight, how='outer')
    # Check for full coverage
    std = normed_weight.groupby(unit_name)['_m'].std()
    try:
        assert std.max() == 0
    except AssertionError:
        if force_coverage:
            # XXX temp patch. `force_coverage` should not be an option.
            normed_weight = normed_weight[normed_weight['_m'] == 3]
            # Fix dtypes from missings
            for col in df_index.columns:
                normed_weight[col] = normed_weight[col].astype(
                    df_index[col].dtype)
        else:
            raise KeyError("All subunits are not fully covered by `df_index`!")
    normed_weight.drop('_m', axis=1, inplace=True)

    return normed_weight
