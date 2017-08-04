from __future__ import division

import numpy as np
import pandas as pd


def add_translated_cols(df, in_cols=None, out_cols=['x', 'y']):
    """
    Translate plane (x + y + z = 1) to R^2.

    Three rotations:
        1) Around z-axis, - pi / 4 [toward xy quadrant 2]
        2) Around x-axis, so all coords have same y
            [transcendental angle, pi / 2 - arcsin(sqrt(2/3))]
        3) Around y-axis, so left vertex is x = 1 and right is z = 1
            (stylistic)
    Together, (.5, .5, 0) -> (0, sqrt(3)/3, sqrt(6)/6)
    Then shift up so bottom of triangle is on new x-axis.
    """
    if in_cols is None:
        cols = ['frac_nohs', 'frac_hs', 'frac_college']
    else:
        cols = in_cols
    xyz = df[cols].copy().values
    arr = _rot_z(xyz)
    arr = _rot_x(arr)
    arr_2d = _rot_2d(arr[:, [0, 2]])
    df[out_cols[0]] = arr_2d[:, 0]
    df[out_cols[1]] = arr_2d[:, 1]
    z_prime = np.sqrt(1 / 6)
    df[out_cols[1]] += z_prime

def _rot_z(arr):
    """ Rotate so (.5, .5, 0) is at (0, sqrt(2)/2, 0) """
    alpha = - np.pi / 4
    rot = np.eye(3)
    rot[0, 0] = np.cos(alpha)
    rot[1, 1] = np.cos(alpha)
    rot[0, 1] = -np.sin(alpha)
    rot[1, 0] = np.sin(alpha)

    return arr.dot(rot)

def _rot_x(arr):
    """ Rotate so plane is vertical (stdev(y) = 0) """
    alpha = np.pi / 2 - np.arcsin(np.sqrt(2 / 3))
    rot = np.eye(3)
    rot[1, 1] = np.cos(alpha)
    rot[2, 2] = np.cos(alpha)
    rot[1, 2] = -np.sin(alpha)
    rot[2, 1] = np.sin(alpha)

    return arr.dot(rot)

def _rot_2d(arr):
    """ Rotate count-clockwise, so no HS is on left, college on right """
    alpha = np.pi * 2 / 3
    ad = np.cos(alpha)
    c = np.sin(alpha)
    rot = np.array([
        [ad, -c],
        [c, ad]
    ])
    out = arr.dot(rot)

    return out


def plot_border(ax):
    right = np.array([np.sqrt(2) / 2, 0])
    left = -right
    top = np.array([0, np.sqrt(3 / 2)])
    # Primary 'axes' (the triangle)
    l_style = '-'
    main_line_prop = dict(c='k', lw=1.5, zorder=0, solid_capstyle='round')
    _draw_line_segment(ax, left, right, l_style, main_line_prop)
    _draw_line_segment(ax, top, right, l_style, main_line_prop)
    _draw_line_segment(ax, top, left, l_style, main_line_prop)
    # Secondary lines
    mid_left = (left + top) / 2
    mid_right = (right + top) / 2
    mid_bot = (left + right) / 2
    l_style = '--'
    aux_line_prop = dict(c='0.25', lw=1, zorder=0)
    _draw_line_segment(ax, mid_left, mid_right, l_style, aux_line_prop)
    _draw_line_segment(ax, mid_left, mid_bot, l_style, aux_line_prop)
    _draw_line_segment(ax, mid_right, mid_bot, l_style, aux_line_prop)
    # Tertiary lines
    l_style = ':'
    aux_line_prop['lw'] = .4
    aux_line_prop['c'] = '0.5'
    _draw_line_segment(ax, .75*top + .25*left, .75*top + .25*right, l_style,
                       aux_line_prop)
    _draw_line_segment(ax, .25*top + .75*left, .25*top + .75*right, l_style,
                       aux_line_prop)
    _draw_line_segment(ax, .25*top + .75*left, .25*right + .75*left, l_style,
                       aux_line_prop)
    _draw_line_segment(ax, .75*top + .25*left, .75*right + .25*left, l_style,
                       aux_line_prop)
    _draw_line_segment(ax, .75*top + .25*right, .25*right + .75*left, l_style,
                       aux_line_prop)
    _draw_line_segment(ax, .25*top + .75*right, .75*right + .25*left, l_style,
                       aux_line_prop)

    # Vertex labels
    vert_shift = .02
    fontsize = 15
    ax.text(left[0], left[1] - vert_shift, r'100% "Less than H.S."',
            fontsize=fontsize,
            horizontalalignment='center',
            verticalalignment='top')
    ax.text(right[0], right[1] - vert_shift, '100% "More than H.S."',
            fontsize=fontsize,
            horizontalalignment='center',
            verticalalignment='top')
    ax.text(top[0], top[1] + .01, '100% "High School"',
            fontsize=fontsize,
            horizontalalignment='center',
            verticalalignment='bottom')

def _draw_line_segment(ax, a, b, l_style, line_prop):
    return ax.plot([a[0], b[0]], [a[1], b[1]], l_style, **line_prop)


def test_data():
    """ Generate full coverage of triangle """
    all_tups = [
        (x, y, 100 - x - y)
        for x in range(100, -1, -1)
        for y in range(100 - x, -1, -1)
    ]
    df = pd.DataFrame(all_tups,
                      columns=['frac_nohs', 'frac_hs', 'frac_college'])
    df /= 100
    df['aermod'] = np.random.rand(df.shape[0])

    df['q'] = pd.qcut(df['aermod'], 3, labels=False)
    df['qn'] = df['q'] / df['q'].max()

    return df
