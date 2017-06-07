from os import path

import pandas as pd
from shapely.geometry import Point
from rtree import index


def load_rtree():
    rtree_path = '../data/block2000_tree'
    exists = (path.isfile(rtree_path + '.dat') and
              path.isfile(rtree_path + '.idx'))
    if exists:
        return index.Rtree(rtree_path)
    else:
        raise(IOError)


if __name__ == '__main__':
    tree_idx = load_rtree()

    # Load and prep ZIP4 data
    zip4 = pd.read_stata('../data/zip4.dta')
    zip4 = zip4.set_index('zip4')[['lat', 'lon']]
    zip4.columns = ['y', 'x']

    # Initialize length of blockID
    out_s = pd.Series(
        ['X' * 20] * len(zip4),
        index=zip4.index,
        name='block2000'
    )
    out_s.index.name = 'zip4'

    for i, (prop_id, row) in enumerate(zip4.iterrows()):
        # Loop through index matches until exact match
        point = Point(row['x'], row['y'])
        # XXX Memory leak here
        intersect = tree_idx.intersection(point.bounds, objects="raw")
        for test_dict in intersect:
            if point.within(test_dict['shp']):
                out_s.at[prop_id] = test_dict['blockid']
                break
        del point, intersect
        if i % 5000 == 0:
            print i

    if type(out_s.iloc[0]) == unicode:
        out_s = out_s.astype(str)

    # Replace unmatched with empty
    out_s[out_s.str[0] == "X"] = ""

    out_s.to_frame('block2000').to_stata('../data/zip4s_block2000.dta')
