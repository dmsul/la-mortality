from os import path

import pandas as pd
import fiona
from shapely.geometry import shape, Point
try:
    from rtree import index
except ImportError:
    pass

from econtools import load_or_build

from util.system import data_path, src_path
from clean.house.main import load_house_sample

counties = (37, 59, 65, 71)
shp_path_model = src_path('shapefiles', 'ca_block2000',
                          'tl_2010_060{}_tabblock00.shp')


@load_or_build(data_path('houses_block2000.pkl'))
def load_houses_block2000():
    """
    Build a 'property_id' to 'block_2000' crosswalk.

    Crosswalks are cached at county level because the `rtree` intersection has
    a memory leak. Just keep running it, it will get there eventually.
    """

    house = _prep_house_data()
    tree_idx = load_rtree()
    houses_blockid = []

    for county in counties:
        df = houses_block2000_by_county(house, county, tree_idx)
        houses_blockid.append(df)

    out_df = pd.concat(houses_blockid)

    out_df.loc[out_df == 'X'*20] = ''

    return out_df

def _prep_house_data():
    house = load_house_sample()
    # Get county fips
    house['county'] = house['bg'].str[3:5].astype(int)
    house = house[['property_id', 'county', 'x', 'y']].drop_duplicates()
    house = house.set_index(['property_id', 'county']).sort_index()
    return house


@load_or_build(data_path('tmp_houses_blockid_{}.pkl'), path_args=[1])
def houses_block2000_by_county(house_df, county, tree_idx):
    county_house = _inloop_county_data(house_df, county)
    return join_loop(county_house, tree_idx)

def _inloop_county_data(df, county):
    county_house = df.loc[pd.IndexSlice[:, county], :]
    county_house.index = county_house.index.droplevel('county')
    return county_house


def join_loop(housexy, tree_idx):
    print "SIZE: {}".format(housexy.shape[0])

    # Initialize length of blockID
    out_s = pd.Series(
        ['X' * 20] * len(housexy),
        index=housexy.index,
        name='block2000'
    )
    out_s.index.name = 'property_id'

    print "Match points to index"
    # Go through each house, get the block
    for i, (prop_id, row) in enumerate(housexy.iterrows()):
        # Loop through index matches until exact match
        point = Point(row['x'], row['y'])
        # XXX Memory leak here
        intersect = tree_idx.intersection(point.bounds, objects="raw")
        for test_dict in intersect:
            if point.within(test_dict['shp']):
                out_s.at[prop_id] = test_dict['blockid']
                break
        if i % 5000 == 0:
            print i

    return out_s


def load_rtree(_rebuild=False, _load=True):
    rtree_path = data_path('block2000_tree')
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
    b2000 = load_houses_block2000()
