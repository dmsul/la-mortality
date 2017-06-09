from os import path

import pandas as pd
import fiona
from shapely.geometry import shape, Point
try:
    from rtree import index
except ImportError:
    pass

counties = ('037', '059', '111')
shp_path_model = path.join(r'd:\data', 'gis', 'census', '2000',
                           'tl_2010_06{}_tabblock00.shp')


def load_rtree(_rebuild=False, _load=True):
    rtree_path = '../data/block2000_tree'
    exists = (path.isfile(rtree_path + '.dat') and
              path.isfile(rtree_path + '.idx'))
    if exists and _load and not _rebuild:
        return index.Rtree(rtree_path)
    else:
        save_rtree(rtree_path)
        return load_rtree()


def save_rtree(rtree_path):
    tree_idx = None
    for county in counties:
        tree_idx = county_rtree(county, tree_idx=tree_idx,
                                rtree_path=rtree_path)
    tree_idx.close()
    del tree_idx


def county_rtree(county, tree_idx=None, rtree_path=None):
    # For using a pre-existing index
    if tree_idx is None:
        tree_idx = index.Rtree(rtree_path)

    print "index {}".format(county)
    filepath = shp_path_model.format(county)
    with fiona.open(filepath) as shp:
        # Put block shapes in Rtree:
        dum_id = 0
        for this_shape in shp:
            tmp_dict = {'blockid': this_shape['properties']['BLKIDFP00'],
                        'shp': shape(this_shape['geometry'])}
            tree_idx.insert(dum_id, tmp_dict['shp'].bounds, obj=tmp_dict)
            dum_id += 1
            if dum_id % 1000 == 0:
                print dum_id

    return tree_idx


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
