import pandas as pd
import numpy as np
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib.pyplot import get_cmap
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib as mpl
from shapely.geometry.point import Point

from util import UTM
from util.gis import utmz11
from util.system import src_path


def map_metric(m, ax_cbar, metric, bin_cutoffs, cbar_label=([], dict()),
               cmap_opts=dict(), alpha=None, shapefile=None):
    """
    Plots a contour map of `metric` using `bin_cutoffs`.

    `alpha`: Series with same index/length as `metric` (needed for join)
    """
    # Massage metric to be plotted
    bin_ids = pd.cut(metric.values.squeeze(), bins=bin_cutoffs, labels=False)

    # Color stuff
    color_list = make_colormap(bin_cutoffs, **cmap_opts)
    make_colorbar(ax_cbar, color_list, bin_cutoffs, cbar_label)

    # Plot `metric` by bin
    if alpha is not None:
        alpha.name = 'alpha'
        unique_alphas = np.unique(alpha)
        alpha_series = dict(zip(unique_alphas,
                                [alpha[alpha == x] for x in unique_alphas]))

    for b in xrange(len(bin_cutoffs) - 1):
        a_bin = metric[bin_ids == b]
        if alpha is None:
            if shapefile:
                patches = []
                for shape_id in a_bin.index:
                    patches.append(Polygon(np.array(shapefile[shape_id])))
                shapes = PatchCollection(patches, facecolor=color_list[b],
                                         edgecolor='w', linewidth=.15)
                m.axes.add_collection(shapes)
            else:
                m.draw_squares(a_bin, color=color_list[b])
        else:
            for this_alpha, alpha_s in alpha_series.iteritems():
                # TODO: must be a better way to restrict by index
                alpha_bin = a_bin.join(alpha_s, how='inner')
                del alpha_bin['alpha']
                m.draw_squares(alpha_bin, color=color_list[b], alpha=this_alpha)


def map_buffer(m, xy, r, **kwargs):
    raw_buffer = Point(*xy).buffer(r)
    df_buffer = pd.DataFrame(list(raw_buffer.exterior.coords),
                             columns=UTM)
    buff_x, buff_y = _get_xy_vecs(df_buffer)

    buffer_patch = Polygon(np.array(
        [m(pt[0], pt[1]) for pt in zip(buff_x, buff_y)]),
        fill=False, **kwargs
    )
    m.ax.add_patch(buffer_patch)


def make_mapscale(m, x_perc, y_perc):
    bounds = m.bounds
    x_loc = (bounds[1] - bounds[0]) * x_perc
    y_loc = (bounds[3] - bounds[2]) * y_perc
    m.drawmapscale(bounds[0] + x_loc, bounds[2] + y_loc,      # location
                   bounds[0], bounds[2],                      # ref for scale
                   10.,
                   barstyle='fancy', labelstyle='simple',
                   fillcolor1='w', fillcolor2='#555555',
                   fontcolor='#555555', zorder=9)


def wrap_CABasemap(plot_data, cbar=False, camap_args=dict()):
    if cbar:
        main_ax = [.05, .05, .75, .9]
    else:
        main_ax = [.05, .05, .95, .95]

    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_axes(main_ax)
    m = CABasemap(plot_data, ax=ax, **camap_args)

    # Extra space for colorbar
    if cbar:
        ax_cbar = fig.add_axes([.83, .20, .03, .6])
        return m, ax_cbar
    else:
        return m


def calc_bounds(*argdfs):
    """Calc a bounding box for many UTM Frames"""
    utm_dfs = [df.reset_index()[UTM] for df in argdfs
               if df is not None]
    mins = pd.concat([np.min(df) for df in utm_dfs], axis=1).T
    maxs = pd.concat([np.max(df) for df in utm_dfs], axis=1).T
    return mins.append(maxs)


def make_colormap(cutoffs, cmap_name='jet', reverse=False):
    # TODO: Test me
    N_bins = len(cutoffs) - 1
    cm = get_cmap(cmap_name)

    # Some colormaps 'descend' in intensity
    ascending_cmaps = ('jet', 'YlOrRd', 'YlOrBr', 'rainbow', 'terrain', 'RdBu',
                       'RdYlGn')
    descending_cmaps = ('autumn', 'afmhot', 'hot', 'gist_heat')
    if cmap_name in ascending_cmaps:
        ascend = True
    elif cmap_name in descending_cmaps:
        ascend = False
    else:
        raise ValueError

    if reverse:
        ascend = not ascend

    if ascend:
        idx_list = range(N_bins)
        shift_d = -1
        shift_n = 0
    else:
        idx_list = range(N_bins, 0, -1)
        shift_d = 2
        shift_n = 1

    color_list = [cm((1.*idx + shift_n)/(N_bins + shift_d)) for idx in idx_list]

    return color_list


def make_colorbar(ax_cbar, color_list, bin_cutoffs, cbar_label):
    new_cmap = mpl.colors.ListedColormap(color_list)
    norm = mpl.colors.BoundaryNorm(bin_cutoffs, new_cmap.N)
    cbar = mpl.colorbar.ColorbarBase(ax_cbar, cmap=new_cmap,
                                     ticks=bin_cutoffs,
                                     norm=norm)
    if cbar_label[0]:
        cbar.set_label(*cbar_label[0], **cbar_label[1])

    return cbar


class CABasemap(Basemap):

    def __init__(self, xy1, xy2=None, ax=None, dist=None, resolution='l',
                 grayscale=False, edge_buffer=None):

        if dist:
            # xy1 is a center, use dist
            xy1 = pd.DataFrame(xy1[UTM].copy()).T.reset_index(drop=True)
            xy1.loc[1, :] = xy1.loc[0, :] + np.array([dist*1000, 0])
            xy1.loc[2, :] = xy1.loc[0, :] - np.array([dist*1000, 0])
            xy1.loc[3, :] = xy1.loc[0, :] + np.array([0, dist*1000])
            xy1.loc[4, :] = xy1.loc[0, :] - np.array([0, dist*1000])

        # Set bounds and center
        bound = self._get_xy_bounds(xy1)
        if xy2 is not None:
            bound2 = self._get_xy_bounds(xy2)
            # XXX This is wrong! want min of mins, max of maxes
            bound = np.maximum(bound, bound2)
        if edge_buffer:
            x_buff = (bound[1] - bound[0]) * edge_buffer
            y_buff = (bound[3] - bound[2]) * edge_buffer
        else:
            # This was previously fixed; keep for now, for compat
            x_buff = 0.015
            y_buff = 0.015
        buff_bounds = (bound[0] - x_buff, bound[1] + x_buff,
                       bound[2] - y_buff, bound[3] + y_buff)
        xbar, ybar = np.mean(bound[:2]), np.mean(bound[2:])
        corners = {
            'llcrnrlon': buff_bounds[0],
            'llcrnrlat': buff_bounds[2],
            'urcrnrlon': buff_bounds[1],
            'urcrnrlat': buff_bounds[3],
            'lon_0': xbar,
            'lat_0': ybar
        }
        self.bounds = buff_bounds
        # Resolution {c, l, i, h, f}
        super(CABasemap, self).__init__(
            projection='tmerc', resolution=resolution, ax=ax, **corners)
        self.axes = plt.gca()

        self.drawcoastlines()
        if grayscale:
            self.drawmapboundary(fill_color='0.8')
            self.fillcontinents(color='1', lake_color='#99ffff',
                                alpha=1, zorder=0)
            county_color = '0.8'
        else:
            # self.drawmapboundary(fill_color='0.8')
            self.fillcontinents(color='0.75', lake_color='#99ffff',
                                alpha=1, zorder=0)
            county_color = 'w'

        self.readshapefile(
            src_path('shapefiles', 'ca_county', 'California_County'),
            'California_County',
            drawbounds=True, color=county_color)
        # self.drawmapboundary(fill_color='#99ffff')

    def _get_xy_bounds(self, xydf):
        """ Get the bounds of a df with lat and long as y and x"""
        x, y = _get_xy_vecs(xydf)
        bounds = (np.min(x), np.max(x), np.min(y), np.max(y))
        return bounds

    def draw_squares(self, utm_df, side=100, **kwargs):
        """
        Draw squares in map coordinates.

        kwargs passed to `axes.add_collection`.
        """

        # Convert center points to map coordinates (meters)
        x, y = _get_xy_vecs(utm_df)
        in_region = _in_region(x, y, self.bounds)
        x, y = x[in_region], y[in_region]
        mxy = zip(*self(x, y))

        # Make collection of squares with edge length `side`
        side = side / 2.
        patches = []
        for this_mxy in mxy:
            this_x, this_y = this_mxy
            x0, x1 = this_x - side, this_x + side
            y0, y1 = this_y - side, this_y + side
            # Order of points matters for drawing
            ordered_points = [(x0, y0), (x0, y1), (x1, y1), (x1, y0)]
            patches.append(Polygon(ordered_points))

        squares = PatchCollection(patches, antialiased=False, **kwargs)
        ax_squares = self.axes.add_collection(squares)
        for n, poly in enumerate(self.coastpolygons[:1]):
            type = self.coastpolygontypes[n]
            if type in [1, 3]:
                p = Polygon(np.asarray(poly).T)
                p.set_color('none')
                self.axes.add_patch(p)
                self.set_axes_limits(ax=self.ax)
                ax_squares.set_clip_path(p)

    def draw_scatter(self, xydf, text_only=None, **kwargs):

        # Restrict to current map
        x, y = _get_xy_vecs(xydf)
        in_region = _in_region(x, y, self.bounds)
        x, y = x[in_region], y[in_region]
        # Convert to map coordinates
        mx, my = self(x, y)

        if text_only:
            size = kwargs.pop('size', 7)
            # Wrap in Series for easy indexing in iterrows loop
            close_xydf = xydf[in_region]
            mx = pd.Series(mx, index=close_xydf.index)
            my = pd.Series(my, index=close_xydf.index)
            for idx, frow in close_xydf.iterrows():
                self.axes.text(
                    mx[idx], my[idx], frow[text_only], zorder=9, size=size,
                    **kwargs)
        else:
            size = kwargs.pop('size', 30)
            self.scatter(mx, my, size, zorder=9, **kwargs)

    def draw_labeled(self, xydf, labelname, offset=1000, **kwargs):

        # Restrict to current map
        x, y = _get_xy_vecs(xydf)
        in_region = _in_region(x, y, self.bounds)
        x, y = x[in_region], y[in_region]
        close_xydf = xydf[in_region]

        # Convert to map coordinates
        mx, my = self(x, y)

        marker_size = kwargs.pop('size', 30)
        text_size = kwargs.pop('size', 10)
        for idx, row in close_xydf.iterrows():
            self.scatter(mx[idx], my[idx], marker_size, **kwargs)
            self.axes.text(mx[idx]+offset, my[idx]+offset,
                           close_xydf.loc[idx, labelname], size=text_size,
                           **kwargs)


def _get_xy_vecs(df):
    return _get_xy_core(df, 'xy')


def _get_utm_vecs(df):
    return _get_xy_core(df, 'utm')


def _get_xy_core(df, xy_or_utm):
    if xy_or_utm == 'xy':
        XY = ['x', 'y']
    elif xy_or_utm == 'utm':
        XY = UTM
    else:
        raise ValueError

    set_XY = set(XY)

    # It's a DF with XY in the columns
    if hasattr(df, 'columns') and set_XY <= set(df.columns):
        x, y = df[XY[0]], df[XY[1]]
    # It's a series or transponsed df
    elif hasattr(df, 'index') and set_XY <= set(df.index):
        x, y = df[XY[0]], df[XY[1]]
    # It's a series or df with XY in the index
    elif hasattr(df, 'index') and set_XY <= set(df.index.names):
        tmp = df.reset_index()
        x, y = tmp[XY[0]], tmp[XY[1]]
    # It's not here, try to convert from UTM
    elif xy_or_utm == 'xy':
        east, north = _get_xy_core(df, xy_or_utm='utm')
        x, y = utmz11(east, north, inverse=True)
    else:
        raise ValueError("{} data not found!".format(xy_or_utm))

    if hasattr(x, 'values'):
        x = x.values
        y = y.values

    return x, y


def _in_region(x, y, bounds):
    x0, x1, y0, y1 = bounds
    in_region = ((x0 < x + 0.01) & (x - 0.01 < x1) &
                 (y0 < y + 0.01) & (y - 0.01 < y1))
    return in_region
