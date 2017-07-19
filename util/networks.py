import pandas as pd
import numpy as np


def equiv_class(indf):

    df = np.maximum(indf, indf.T)
    df = make_transitive(df)
    df = df.drop_duplicates()

    outdf = pd.DataFrame()
    for idx, row in df.iterrows():
        names = np.sort(row[row].index.values)
        outdf = outdf.append(pd.Series(names, name=names[0]))

    try:
        outdf = outdf.astype(df.index.dtype)
    except ValueError as ve:
        if not np.isnan(outdf).any().any():
            raise ve

    return outdf


def make_transitive(inarr):
    """
    Make a non-directional relation matrix transitive; i.e., indicator matrix
    for 'a can get to b' by some path.
    """
    is_df = isinstance(inarr, pd.core.frame.DataFrame)

    if is_df:
        arr = inarr.values.copy()
    else:
        arr = inarr.copy()

    I, J = arr.shape
    for i in xrange(I):
        linked_idx = np.where(arr[i, :])[0].tolist()
        for j in linked_idx:
            if i == j:
                continue
            arr[i, :] = np.maximum(arr[i, :], arr[j, :])
            new_linked_idx = [x for x in np.where(arr[i, :])[0].tolist()
                              if x not in linked_idx]
            linked_idx += new_linked_idx

    if is_df:
        output = pd.DataFrame(arr, index=inarr.index, columns=inarr.columns)
    else:
        output = arr

    return output
