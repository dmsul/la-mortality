from __future__ import division

import pandas as pd
import numpy as np

from econtools import load_or_build

from util import UTM
from util.system import data_path, bulk_path
from clean.pr2 import load_stacks

# XXX This `MAX_RADIUS` Should be at least `atmods.env.ALTMAXDIST`, but making
# them come from the same place would require a submodule further down because
# `atmods` imports `clean`.
MAX_RADIUS = 30
# XXX This `GRID_SIZE` is also more of a project-level global; a `util`-level
# config file probably warranted.
GRID_SIZE = 100


@load_or_build(data_path('grid_master.p'))
def load_master_grid(_rebuild_down=False):

    all_firms_utm = load_stacks().groupby('facid')[UTM].mean()
    relative_grid = make_generic_grid()
    grid_utm_index = pd.DataFrame()
    # Make a grid for each firm, take union as you go
    i = 1
    for facid, this_firms_utm in all_firms_utm.iterrows():
        print "Facid: {}".format(facid)
        df = make_firms_grid(facid, relative_grid, this_firms_utm,
                             _rebuild=_rebuild_down)
        grid_utm_index = grid_utm_index.append(df)
        if i == 50:
            grid_utm_index.drop_duplicates(inplace=True)
            i = 1
        else:
            i += 1
    grid_utm_index.drop_duplicates(inplace=True)
    df = grid_utm_index

    return df


@load_or_build(bulk_path('grids', 'grid_raw_{}.p'), path_args=[0])
def make_firms_grid(facid, relative_grid=None, firm_utm=None):
    """ Center `relative_grid` on `firm_utm`. `facid` is passed to filename """
    # Wrappers for use outside of `load_master_grid` loop
    if relative_grid is None:
        relative_grid = make_generic_grid()
    if firm_utm is None:
        firm_utm = load_stacks(facid)[UTM].drop_duplicates().squeeze()

    firms_east_rounded = round_nearest(firm_utm['utm_east'], GRID_SIZE)
    firms_north_rounded = round_nearest(firm_utm['utm_north'], GRID_SIZE)
    firms_grid = relative_grid.copy()
    firms_grid['utm_east'] += firms_east_rounded
    firms_grid['utm_north'] += firms_north_rounded

    return firms_grid


def make_generic_grid():
    r_buff = MAX_RADIUS + 4 * GRID_SIZE / 1000      # Add buffer
    _circle = lambda x: _circle_y(x, r_buff)        # noqa
    # 1) Make the triangle under y = x for x \in [0, `square_x`]
    square_x = round_nearest(r_buff * 1000 / np.sqrt(2), GRID_SIZE)
    square_y = round_nearest(_circle(square_x), GRID_SIZE)
    assert square_x == square_y
    del square_y
    triangle = pd.DataFrame(
        [(x, y)
         for x in xrange(0, square_x + 1, GRID_SIZE)
         for y in xrange(0, x + 1, GRID_SIZE)],
        columns=UTM
    )

    # 2) Count down from MAX_RADIUS by GRID_SIZE, and do that column
    max_r_m = int(r_buff * 1000)
    curve = pd.DataFrame(
        [(x, y)
         for x in xrange(max_r_m, square_x, -GRID_SIZE)
         for y in xrange(_circle(x), -GRID_SIZE, -GRID_SIZE)],
        columns=UTM
    )
    # 3) Reflect over y = x
    full = triangle.append(curve)
    reflect = full.copy()
    reflect.rename(columns={'utm_east': 'utm_north', 'utm_north': 'utm_east'},
                   inplace=True)
    full = full.append(reflect)
    # 4) Reflect over other quadrants.
    q2 = full.copy()
    q2['utm_east'] *= -1
    q3 = q2.copy()
    q3['utm_north'] *= -1
    q4 = q3.copy()
    q4['utm_east'] *= -1

    full = full.append(q2).append(q3).append(q4).astype(np.int32)
    full.drop_duplicates(inplace=True)

    return full


def _circle_y(x, r_buff):
    """ Return `y` for circle radius `r` at `x` """
    x_km = x / 1000
    y = np.sqrt(r_buff*r_buff - x_km*x_km) * 1000
    round_y = round_nearest(y, GRID_SIZE)
    return round_y


def round_nearest(x, val):
    return int(np.around(x / val) * val)


if __name__ == "__main__":
    df = load_master_grid(_rebuild=True, _rebuild_down=True)
