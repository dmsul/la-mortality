from __future__ import division

import pandas as pd
import numpy as np

import econtools as ect
from econtools import load_or_build
from econtools.geo.krig import kriging_weights, check_variogram
from econtools.metrics.regutil import winsorize

from util import UTM
from util.system import data_path
from util.distance import center_data, getdist
from util.buildblocks import swap_index, build_avg
from clean import (load_geounit, load_bgdata, load_blocks, load_blocks_wzip,
                   load_bg_area, load_houses_places, std_cities, load_monitors,
                   load_houses_block2000)
from clean.pr2 import load_stacks
from atmods.io import load_full_exposure as aio_load_full_exposure
from atmods.io import (_get_all_facids, load_firm_normed_exp,
                       formatted_firms_emission_grams_sec)
from atmods.env import MAXDIST
from analysis.regutils import (region_def_by_firm,)

MAX_YEAR_BUILT = 1995
IV_YEARS = range(1995, 1996+1)
SAMPLE_REGION_DIST = 10.
YEAR0 = 1997
YEART = 2005

# REGION_POINTS = pd.Series((380959, 3740217), index=UTM)

use_citytrend = False
GRID_KM = 10.
GRID_SHIFT = {'utm_east': .4, 'utm_north': .4}

MIN_BG_POP = 400

ed_cats = ('nohs', 'hs', 'college')
ed_names = ["Less than HS", "High School", "More than HS"]
z_ed_frac = ['z_frac_' + ed for ed in ed_cats]
ed_name_dict = dict(zip(ed_cats, ed_names))


def prep_regdata_bg(**kwargs):

    df = _prep_regdata_bg_guts(**kwargs)

    pre_post_vars = df.filter(regex='.*_2000_post').columns.tolist()
    std_I = df.filter(like='_I').columns.tolist()
    _I = ['post'] + pre_post_vars + std_I

    return df, _I


@load_or_build(data_path('regdata', 'bg_prepped.p'))
def _prep_regdata_bg_guts():

    regdata = regionspec(SAMPLE_REGION_DIST, region='sw', obsunit='bg')

    # Drop weird guys
    regdata = regdata[regdata['bg'] != '060375746011']  # Cal State Dorms
    regdata = regdata[regdata['bg'] != '060372961002']  # Long Beach Docks
    regdata = regdata[regdata['bg'] != '060375747009']  # Long Beach VA

    # Add area
    regdata = regdata.join(load_bg_area(), on='bg')

    # year dummies
    regdata['post'] = (regdata['year'] >= 2001).astype(int)

    # Trend grid
    regdata = _trend_grid(regdata)
    regdata = _trend_grid_interactions(regdata, bg=True)

    # IV
    regdata['aermod_pre'] = regdata.filter(like='aermod_199').mean(axis=1)
    regdata['aermod_pre_post'] = regdata['aermod_pre'] * regdata['post']

    # New variables, etc.
    regdata = _bg_ed_prep(regdata)

    regdata = _bg_income_bins(regdata)

    regdata['pop_per_sqmi'] = regdata['pop'] / regdata['area']
    regdata['frac_white'] = regdata['race_white'] / regdata['pop']
    regdata['frac_black'] = regdata['race_black'] / regdata['pop']
    regdata['frac_hisp'] = regdata['race_hisp'] / regdata['pop']
    regdata['frac_bh'] = regdata['frac_black'] + regdata['frac_hisp']
    regdata['frac_renter'] = regdata['hunit_renter'] / regdata['hunit']

    regdata['lpop'] = np.log(regdata['pop'])
    regdata['lhh'] = np.log(regdata['households'])
    regdata['lhhincmed'] = np.log(regdata['hhincmed'])
    regdata['lfrac_white'] = np.log(regdata['frac_white'])
    regdata['lpop_per_sqmi'] = np.log(regdata['pop_per_sqmi'])
    regdata['lrent'] = np.log(regdata['rent_median'])
    regdata['lhunit'] = np.log(regdata['hunit'])
    regdata['led_nohs'] = np.log(regdata['ed_nohs'])
    regdata['led_hs'] = np.log(regdata['ed_hs'])
    regdata['led_college'] = np.log(regdata['ed_college'])
    for x in range(1, 4):
        varname = 'incbin_{}'.format(x)
        regdata['ln_' + varname] = np.log(regdata[varname])

    regdata.replace(-np.inf, np.nan, inplace=True)

    # House loan vars (not currently used)
    regdata['tract2000'] = regdata['bg'].str[:-1]
    regdata = _merge_loans(regdata)

    # Add pre-period SES stuff
    regdata['age25'] = regdata[['ed_nohs', 'ed_hs', 'ed_college']].sum(axis=1)
    preperiod_vars = ['pop', 'households', 'lhhincmed',
                      'frac_nohs', 'frac_hs', 'age25',
                      'frac_white', 'frac_hisp', 'frac_black', 'hunit',
                      'frac_incbin_1', 'frac_incbin_2',
                      ]
    regdata = _year0_ses_post(regdata, preperiod_vars)

    # Aermod interactions
    ed_level = ['ed_nohs', 'ed_hs', 'ed_college']
    ed_frac = ['frac_nohs', 'frac_hs', 'frac_college']
    ed = ed_level + ed_frac
    ed_pre = regdata.set_index('bg').loc[(regdata['year'] == 2000).values, ed]
    ed_pre.rename(columns=lambda x: x + '_2000', inplace=True)
    regdata = regdata.join(ed_pre, on='bg')
    for ed in ed:
        regdata['z_' + ed] = (regdata[ed + '_2000'] *
                              regdata['aermod_pre_post'])

    # Restrict by year 2000 pop
    regdata = regdata[regdata['pop2000'] >= MIN_BG_POP]

    regdata = regdata.reset_index(drop=True)

    # winsorize
    # regdata = winsorize(regdata, 'aermod_pre', p=(0, .99))

    return regdata

def _bg_ed_prep(regdata):
    dropout = _ed_list((0, 4, 6, 8, 9, 10, 11, 12))
    hs = _ed_list(('_hs', '_coll_1', '_coll_nod', '_aa'))
    college = _ed_list(('_ba', '_ma', '_jd', '_phd'))
    regdata['ed_nohs'] = regdata[dropout].sum(axis=1)
    regdata['ed_hs'] = regdata[hs].sum(axis=1)
    regdata['ed_college'] = regdata[college].sum(axis=1)
    regdata['ed_total'] = regdata.eval('ed_nohs + ed_hs + ed_college')
    for ed in ('nohs', 'hs', 'college'):
        regdata['frac_{}'.format(ed)] = regdata.eval(
            'ed_{} / ed_total'.format(ed))
    regdata.drop(dropout, axis=1, inplace=True)
    regdata.drop(hs, axis=1, inplace=True)
    regdata.drop(college, axis=1, inplace=True)

    return regdata

def _ed_list(catlist):
    varlist = ['{}_ed{}'.format(sex, str(ed))
               for sex in ('male', 'female')
               for ed in catlist]
    return varlist

def _year0_ses_post(regdata, preperiod_vars):
    tmp_2000 = regdata.loc[regdata['year'] == 2000,
                           preperiod_vars + ['bg']].copy()
    tmp_2000.set_index('bg', inplace=True)
    tmp_2000.rename(columns=lambda x: x + '_2000_post', inplace=True)
    regdata = regdata.join(tmp_2000, on='bg')
    for prevar in preperiod_vars:
        regdata.loc[regdata['year'] == 2000, prevar + '_2000_post'] = 0

    return regdata

def _bg_income_bins(df):
    """
    Uses `hhinc_*` variables to create:
        1) `incbin_*` (sum of `hhinc_*` counts)
        2) `frac_inc_bin_*`
        3) `inc_p{low,high}`, the `plow` and 1 - `plow` percentile of BG's
           income
    """
    if 'bg2000' == df.index.name:
        # XXX SUPER janky so the merge to houses works!
        df = df.reset_index()
        df['year'] = 2000
        merge_on = ['bg2000', 'year']
        drop_tmp_year = True
    else:
        merge_on = ['bg', 'year']
        drop_tmp_year = False
    # Isolate household income vars, rename as int's
    hhinc = df[merge_on + df.filter(like='hhinc_').columns.tolist()]
    hhinc = hhinc.set_index(merge_on)
    hhinc = hhinc.rename(columns=_inc_as_int).sort_index(axis=1)
    # Consolidate to income bins, sum household counts w/in bins
    inc_bin = hhinc.rename(columns=_incbins)
    inc_bin = inc_bin.T.groupby(level=0).sum().T
    inc_bin = inc_bin.rename(columns=lambda x: 'incbin_{}'.format(x))
    # Do "fraction in bin" variables
    totals = inc_bin.sum(axis=1)
    inc_bin_frac = inc_bin.divide(totals, axis=0)
    inc_bin_frac = inc_bin_frac.rename(columns=lambda x: 'frac_' + x)

    # Put bin counts and 'frac in bin' together
    out_df = inc_bin.join(inc_bin_frac)
    assert len(out_df) == len(inc_bin)

    # Get year 2000 values separately
    frac_2000 = inc_bin_frac.sort_index().loc[pd.IndexSlice[:, 2000], :]
    frac_2000 = frac_2000.rename(columns=lambda x: x + '_2000')
    frac_2000.index = frac_2000.index.droplevel('year')
    out_df = out_df.join(frac_2000)
    assert len(out_df) == len(inc_bin)

    # w/in BG income percentiles
    plow = .15
    out_df['inc_plow'] = inc_ptile(hhinc, plow)
    out_df['inc_phigh'] = inc_ptile(hhinc, 1 - plow)

    if drop_tmp_year:
        # XXX see note about jankiness above
        out_df = out_df.reset_index('year', drop=True)
        merge_on = ['bg2000']

    df = df.join(out_df, on=merge_on)

    return df

def _inc_as_int(x_str):
    return int(x_str.replace('hhinc_', ''))

def _incbins(x):
    if x <= 30:
        return 1
    elif 30 < x <= 60:
        return 2
    else:
        return 3

def inc_ptile(hhinc, ptile):
    hhinc = hhinc.sort_index(axis=1)
    cdf = hhinc.divide(hhinc.sum(axis=1), axis=0).cumsum(axis=1)
    return (cdf > ptile).idxmax(axis=1)


def prep_bg_rent_regs():
    df, _I = prep_regdata_bg()

    # Drop bad rent guys
    df['bad_rent'] = df['rent_median'].isin((0, 2001))  # Error code & topcode
    has_bad_rent = df.groupby('bg')['bad_rent'].max()
    df = df.join(has_bad_rent.to_frame('has_bad_rent'), on='bg')
    df = df[~df['has_bad_rent']]
    df.drop('has_bad_rent', axis=1, inplace=True)

    # Add `rent_median to controls
    rent_2000 = df.loc[df['year'] == 2000, ['bg', 'rent_median']]
    rent_2000 = rent_2000.set_index('bg').squeeze()
    df = df.join(rent_2000.to_frame('rent_median_2000_post'), on='bg')
    df.loc[df['year'] == 2000, 'rent_median_2000_post'] = 0
    _I += ['rent_median_2000_post']

    # Add `renters in 2000` variable
    df.set_index('bg', inplace=True)
    renters_2000 = df.loc[df['year'] == 2000, 'hunit_renter']
    df = df.join(renters_2000.to_frame('renters_2000'))
    df.reset_index(inplace=True)

    return df, _I


# Stacked GeoDD prep
RAW_STACKED_DIST_MAX_MI = 6


def prep_stacked_sale_data(r0, r1, **kwargs):
    df = _prep_stacked_sale_data_guts(r0, r1, **kwargs)
    _I = df.filter(like='_I').columns.tolist()

    return df, _I


@load_or_build(data_path('regdata', 'sale_stacked_prepped_t{}c{}.p'),
               path_args=[0, 1])
def _prep_stacked_sale_data_guts(r0, r1, _load=True, _rebuild=False):
    """ r0=0 returns full data w/ no radii. """

    if r1 > RAW_STACKED_DIST_MAX_MI:
        err_str = "`r1` value ({}) exceeds max dist in data ({})"
        raise ValueError(err_str.format(r1, RAW_STACKED_DIST_MAX_MI))

    df = stacked_sale_data()

    # Set treatment/control radii (restricts sample)
    if r0 != 0:
        df = _set_radii(df, r0=r0, r1=r1)

    # Merge in housing variables of interest
    houses, _I = prep_regdata_sale()
    join_id = ['property_id', 'year', 'quarter']
    houses.set_index(join_id, inplace=True)
    keepvars = ['lnp', 'aermod', 'aermod_pre_post', 'hgrid',
                'grid_id', 'bg'] + _I
    df = df.join(houses[keepvars], on=join_id)

    return df

def _set_radii(df, r0=1, r1=2):
    """
    Set the radii for the GeoDD.
    NOTE: args are in miles, but all distances in kilometers.
    """
    # Convert to kilometers
    r0_km = r0*1.6
    r1_km = r1*1.6

    df['samp'] = df['dist'] <= r1_km
    df['near'] = (df['dist'] <= r0_km).astype(int)
    df['post'] = (df['year'] >= 2001).astype(int)
    df['near_post'] = df['near'] * df['post']
    year0, yearT = df['year'].min(), df['year'].max()
    for y in range(year0 + 1, yearT + 1):
        varname = 'near_a{}'.format(y)
        df[varname] = df['near'] * (df['year'] == y)

    return df[df['samp']]


@load_or_build(data_path('regdata', 'sale_stacked.p'))
def stacked_sale_data():

    stacks = load_stacks()
    facids = stacks['facid'].unique()
    utms = stacks[['facid'] + UTM].drop_duplicates().set_index('facid')
    assert len(facids) == utms.shape[0]

    df = _stack_em_up(facids, utms, RAW_STACKED_DIST_MAX_MI)
    df = _get_their_rawexposure(df, facids)
    # Merge in firm's emissions
    emit = formatted_firms_noxgs().fillna(0).stack('quarter').to_frame('noxgs')
    emit = emit.sort_index()
    df = df.reset_index().set_index(['facid', 'year', 'quarter']).sort_index()
    df = df.join(emit)
    df['firms_aermod'] = df['raw_exposure'] * df['noxgs']

    df.reset_index(inplace=True)

    return df

def _stack_em_up(facids, utms, max_miles):
    houses, _I = prep_regdata_sale()
    h_utm = houses[UTM].drop_duplicates()
    new_idx = pd.DataFrame()
    for facid in facids:
        print "Facid: {}".format(facid)
        this_utm = utms.loc[facid]
        dist = getdist(h_utm, this_utm).squeeze().values
        in_samp = dist <= max_miles*1.6
        this_h_df = h_utm[in_samp].copy()
        this_h_df['facid'] = facid
        this_h_df['dist'] = dist[in_samp]
        new_idx = new_idx.append(this_h_df)

    keepvars = ['property_id', 'year', 'quarter'] + UTM
    df = pd.merge(houses[keepvars], new_idx, on=UTM, how='inner')
    df = ect.group_id(df, cols=['property_id', 'facid'], merge=True)
    return df

def _get_their_rawexposure(df, facids):
    df.rename(columns={'aermod': 'all_aermod'}, inplace=True)
    facids_in_df = tuple(df['facid'].unique())
    df.set_index(UTM + ['quarter', 'facid'], inplace=True)
    df.sort_index(inplace=True)
    exp_name = 'raw_exposure'
    df[exp_name] = np.nan
    raw_exposure = all_raw_exposure('house', 'aermod')
    raw_exposure.columns.name = 'facid'
    raw_exposure.name = exp_name

    for facid in facids:
        print "Exp facid {}".format(facid)
        if facid not in facids_in_df:
            print "skipped!"
            continue
        this_facid = raw_exposure[facid].to_frame(exp_name)
        this_facid['facid'] = facid
        this_facid.set_index('facid', append=True, inplace=True)
        df.update(this_facid)

    return df
    pass


# Sale prep
def prep_regdata_sale(**kwargs):
    fe = kwargs.pop('fe', True)
    year0 = kwargs.pop('year0', YEAR0)
    yearT = kwargs.pop('yearT', YEART)
    if fe:
        df = _prep_regdata_sale_guts(year0=year0, yearT=yearT, **kwargs)
    else:
        df = _prep_regdata_sale_nofe_guts(year0=year0, yearT=yearT, **kwargs)
    _I = df.filter(like='_I').columns.tolist()
    return df, _I


@load_or_build(data_path('regdata', 'sale_prepped_{}_{}.p'),
               path_args=['year0', 'yearT'])
def _prep_regdata_sale_guts(year0=YEAR0, yearT=YEART):
    df = prep_regdata_core(year0, yearT, fe=True, winsor=True)
    return df


@load_or_build(data_path('regdata', 'sale_nofe_prepped_{}_{}.p'),
               path_args=['year0', 'yearT'])
def _prep_regdata_sale_nofe_guts(year0=YEAR0, yearT=YEART):
    df = prep_regdata_core(year0, yearT, fe=False, winsor=True)
    return df


def prep_regdata_core(year0, yearT, fe, winsor):
    """
    Prep main regdata for house price regressions.
    """

    regdata = regionspec(SAMPLE_REGION_DIST, region='sw')

    regdata = regdata[regdata['year'].isin(range(year0, yearT+1))].copy()

    regdata = regdata.join(load_houses_block2000(), on='property_id')
    regdata['bg2000'] = regdata['block2000'].str[:-3]
    regdata['tract2000'] = regdata['block2000'].str[:-4]

    regdata = _merge_bgdemogs(regdata)
    regdata = _merge_loans(regdata)

    _I = []
    # year dummies
    regdata = _year_dummies(regdata, year0, yearT, _I)
    # hgrid
    regdata = ect.group_id(regdata, cols=UTM, name='hgrid', merge=True)
    # Trend grid
    regdata['t'] = (regdata['year'] - regdata['year'].min() +
                    (regdata['quarter'] - 1) / 4)
    regdata['t2'] = regdata['t'] ** 2
    if not use_citytrend:
        regdata = _trend_grid(regdata)
        regdata = _trend_grid_interactions(regdata)
    else:
        regdata = _trend_city(regdata, _I)
    # SES time trends
    colname = '_I_{}_{}'
    ses_cols = ('loan_ltv', 'intrate', 'linc')
    for col in ses_cols:
        for t in ('t', 't2'):
            regdata[colname.format(col, t)] = regdata[col] * (regdata[t])
            _I.append(colname.format(col, t))

    # IV
    regdata['post'] = (regdata['year'] >= 2001).astype(int)
    for model in ['aermod', 'tria5', 'unif2']:
        model_pre = '{}_pre'.format(model)
        model_pre_post = '{}_pre_post'.format(model)
        # XXX !!!!!!!!! rando regex selection of IV_YEARS
        pre_year_filter = '{}_199'.format(model)
        regdata[model_pre] = regdata.filter(like=pre_year_filter).mean(axis=1)
        regdata[model_pre_post] = regdata[model_pre] * regdata['post']
        for y in range(year0+1, yearT+1):
            varname = model_pre + '_a{}'.format(y)
            regdata[varname] = regdata[model_pre]*((regdata['year'] == y))

    if winsor:
        regdata = winsor_fe(regdata, by=['lnp'], p=(0, .999), fe=fe)

    # Drop constant stuff
    regdata, _I = _drop_constantvars(regdata, _I)

    # Reset Index after all the cleaning
    regdata = regdata.reset_index(drop=True)

    return regdata

def _merge_bgdemogs(regdata):
    bgdata = load_bgdata(2000)
    bgdata.index.name = 'bg2000'
    bgdata['frac_black'] = bgdata['race_black'] / bgdata['pop']
    bgdata['frac_hispanic'] = bgdata['race_hisp'] / bgdata['pop']
    bgdata['linc'] = np.log(bgdata['hhincmed'])
    bgdata['frac_renter'] = bgdata['hunit_renter'] / bgdata['hunit']

    bgdata = _bg_ed_prep(bgdata)
    bgdata = _bg_income_bins(bgdata)
    bgdata = bgdata.set_index('bg2000')   # This is undone by `_bg_income_bins`

    bgvars = ['frac_black', 'frac_hispanic', 'linc', 'frac_renter', 'hhincmed',
              'frac_nohs', 'frac_hs', 'frac_college',
              'incbin_1', 'incbin_2', 'incbin_3',
              'inc_plow', 'inc_phigh',
              ]
    bgvars += bgdata.filter(like='frac_incbin').columns.tolist()
    regdata = regdata.join(bgdata[bgvars], on='bg2000')
    regdata['frac_black'].fillna(0, inplace=True)

    regdata = regdata[regdata['hhincmed'] != 0]
    return regdata

def _merge_loans(regdata, year=2000):
    house = load_geounit('house')
    house.rename(columns={'estimated_interest_rate_1': 'intrate'},
                 inplace=True)

    keep = (house['year'] == year) & (house['lnp'].notnull())
    # XXX This is temporary to handle new and old `house_sample`
    if 'loan_ltv' in house.columns:
        house = house.loc[keep,
                          ['property_id', 'loan_ltv', 'intrate']]
    else:
        house = house.loc[keep,
                          ['property_id', 'p', 'origination_loan', 'intrate']]
        house['loan_ltv'] = house['origination_loan'] / house['p']
    house = house.join(load_houses_block2000(), on='property_id')
    house['bg2000'] = house['block2000'].str[:-3]
    house['tract2000'] = house['block2000'].str[:-4]
    merge_on = 'tract2000'
    bg_ltv = house.groupby(merge_on)[['loan_ltv', 'intrate']].mean()

    regdata = regdata.join(bg_ltv, on=merge_on)

    return regdata

def _year_dummies(regdata, year0, yearT, _I):
    for y in range(year0, yearT + 1):
        for q in range(1, 5):
            if y == year0 and q == 1:
                continue
            varname = '_Iyear_{}q{}'.format(y, q)
            _I.append(varname)
            regdata[varname] = (
                (regdata['quarter'] == q) & (regdata['year'] == y)).astype(int)
    return regdata

def _trend_grid(regdata):
    """ Adds `utm_{UTM}_20`, the rounded utm value, and `grid_id` """
    grid_m = GRID_KM * 1000     # From global value
    for utm in UTM:
        regdata[utm + '_20'] = (
            np.around(
                # Shift to line up w/ cities
                (regdata[utm] + GRID_SHIFT[utm]*grid_m) / grid_m) * GRID_KM
        ).astype(int)
    # Grid ID based on rounded UTM
    # NOTE: need as many digits as very, will need more if grid size gets much
    # smaller than 10 km.
    regdata['grid_id'] = regdata.apply(
        lambda x: (
            # First two digits of utm_north are always '37', leave em out
            str(x['utm_east_20'])[:3] + '-' + str(x['utm_north_20'])[2:4]
        ),
        axis=1
    )

    return regdata

def _trend_grid_interactions(regdata, bg=False):
    unique_grids = regdata['grid_id'].unique()
    tname = '_Igrid{}_post' if bg else '_Igrid{}_{}'
    for idx, grid_id in enumerate(unique_grids):
        # Drop first one (colinearity)
        if idx == 0:
            continue
        this_grid = (regdata['grid_id'] == grid_id).astype(int)
        if bg:
            this_tname = tname.format(grid_id)
            regdata[this_tname] = this_grid * regdata['post']
        else:
            for t in ('t', 't2'):
                this_tname = tname.format(grid_id, t)
                regdata[this_tname] = this_grid * regdata[t]

    return regdata


def _trend_city(regdata, _I):
    regdata = _city_group(regdata)
    cities = regdata['city'].unique()
    tname = '_Icity{}_{}'
    for city in cities:
        if city == cities[0]:
            continue
        this_city = (regdata['city'] == city).astype(int)
        for t in ('t', 't2'):
            this_tname = tname.format(city[:3], t)
            regdata[this_tname] = this_city * regdata[t]
            _I.append(this_tname)

    return regdata

def _city_group(regdata):
    """
    Groups neighboring cities together.

    This is only partially complete and abandoned in favor of a selectively
    placed grid. (10km UTM grid shifted by 1 km., i.e. starting at 341000 east,
    3731000 north.) This grid lines up very well with existing city borders and
    takes care of non-convexities and weird outliers.

    If/when this needs to be finished:
    1) Make sure all the houses get matched to a raw city
    2) Cut bigger cities like LA and Long Beach by UTM (this is already kind of
        started).
    3) Re-evaluate the assignments made in `matches`.
    3) Group cities together as a tuple inside `second_match`, the following
        code will great super-cities from the tuples.
    """
    cities = load_houses_places().set_index('property_id')
    regdata = regdata.join(cities, on='property_id')

    # FIXME temporary! bad house data build
    regdata = regdata[regdata['city'].notnull()]
    regdata = regdata[regdata['city'] != '']

    std_cities(regdata)
    regdata['orig'] = regdata['city'].copy()

    # Manual Matches (on geography and `lnp`/demographics)
    # FIXME this was pre-grid, make sure I don't want some of these broken up
    matches = {
        'WESTCHESTER': 'EL SEGUNDO',    # Best `lnp` match w/in 5km
        'SUNSET BEACH': 'HUNTINGTON BEACH',     # Annexed
        'LENNOX': 'HAWTHORNE',                  # Neighbor, both low `lnp`
        'PLAYA DEL REY': 'VENICE',              # Dist and `lnp` match (MARINA)
        'ARTESIA': 'CERRITOS',                  # Near enclave
        'NEWPORT BEACH': 'HUNTINGTON BEACH',    # Annexed
        'ROSSMOOR': 'SEAL BEACH',               # Neighbor, both high `lnp`
        'ANAHEIM': 'CYPRESS',                   # Neighbor, both low `lnp`
        'MIDWAY CITY': 'WESTMINSTER',           # Neighbor, low `lnp`, Asian
        'SIGNAL HILL': 'LONG BEACH',            # Enclave
        'ROLLING HILLS': 'PALOS VERDES',        # Really like one city
        'ROLLING HILLS ESTATES': 'PALOS VERDES',
        'RANCHO PALOS VERDES': 'PALOS VERDES',
        'PALOS VERDES ESTATES': 'PALOS VERDES',
        'PALOS VERDES PENINSULA': 'PALOS VERDES',
        'HAWAIIAN GARDENS': 'CERRITOS',         # Neighbor, both low `lnp`
        'MARINA DEL REY': 'VENICE',
        'SANTA MONICA': 'VENICE',
        'BUENA PARK': 'CYPRESS',
        'STANTON': 'CYPRESS',
        'LA PALMA': 'CERRITOS',
        'HARBOR CITY': 'LOMITA',
        'WILMINGTON': 'LOMITA',
        'PARAMOUNT': 'BELLFLOWER',
        'DOWNEY': 'BELLFLOWER',
    }
    regdata['city'].replace(matches, inplace=True)
    regdata['orig2'] = regdata['city'].copy()

    # Second round manual matches (on raw "slap 'em together")
    la_east = ((regdata['city'] == 'LOS ANGELES') &
               (regdata['utm_east'] >= 381000))
    regdata.loc[la_east, 'city'] = 'LA EAST'

    second_match = (
        ('SEAL BEACH', 'HUNTINGTON BEACH', 'GARDEN GROVE', 'WESTMINSTER',
         'LOS ALAMITOS'),
        ('CYPRESS', 'CERRITOS', 'LAKEWOOD',),
        ('LONG BEACH',),
        ('PALOS VERDES', 'SAN PEDRO',),
        ('COMPTON', 'SOUTH GATE', 'LYNWOOD', 'BELLFLOWER',),
        ('CARSON', 'LOMITA',),
        ('TORRANCE', 'REDONDO BEACH',),
        ('EL SEGUNDO',),
        ('HAWTHORNE', 'LAWNDALE', 'INGLEWOOD', 'GARDENA'),
        ('MANHATTAN BEACH', 'HERMOSA BEACH'),
        ('LOS ANGELES', 'CULVER CITY', 'VENICE',),
    )

    if 0 == 1:
        for c in second_match:
            regdata['city'].replace(list(c), list(c[:1])*len(c), inplace=True)

    if use_citytrend:
        city_summ = regdata.groupby('city').agg(
            {'property_id': np.count_nonzero,
             'lnp': np.mean,
             'utm_east': np.mean,
             'utm_north': np.mean})
        city_summ = city_summ.sort_values('lnp')
        return regdata, city_summ
    else:
        return regdata


def add_ed_vars(df):
    for ed in ('nohs', 'hs', 'college'):
        frac = 'frac_' + ed
        # Local quadratic trends
        for t in ('t', 't2'):
            if ed == 'college':
                continue
            df['_I_{}_{}'.format(ed, t)] = df[frac] * df[t]
        # Z * ed
        df['z_frac_' + ed] = df['aermod_pre_post'] * df[frac]


# Aux methods (many should go into `econtools`!)
def _drop_constantvars(regdata, _I):
    dropped = []
    for col in _I:
        if regdata[col].std() == 0:
            regdata.drop(col, axis=1, inplace=True)
            dropped.append(col)
    _I = [x for x in _I if x not in dropped]

    return regdata, _I


def winsor_fe(regdata, by, p, fe=True):
    if fe:
        count = regdata.groupby('property_id').size()
        regdata = regdata.join(count.to_frame('_T'), on='property_id')
        regdata = regdata[regdata['_T'] > 1].reset_index(drop=True)
        del regdata['_T']

    regdata = winsorize(regdata, by=by, p=p)

    return regdata


def regionspec(maxdist, obsunit='sale', region='', _rebuild_down=False):
    regdata = prep_geounit(obsunit, _rebuild=_rebuild_down)

    regdata = regdata[regdata['year'] != 1995]

    if 'yr_blt' in regdata.columns:
        regdata = regdata[regdata['yr_blt'] <= MAX_YEAR_BUILT]

    # Restrict to region
    regdata = center_sw(regdata)

    return regdata


def center_sw(regdata, maxdist=SAMPLE_REGION_DIST, convex=False):
    df = regdata.reset_index()
    firmlist = region_def_by_firm(None)
    region_points = load_stacks(firmlist).groupby('facid')[UTM].mean()
    _push_out_southwest(region_points)
    # Main "distance from seed firms" criterion
    in_region = getdist(df, region_points, within=maxdist).max(axis=1)

    if convex:
        # Add area between circles
        edge_seeds = (9755, 800123, 115394)
        for i in range(len(edge_seeds) - 1):
            firm1, firm2 = edge_seeds[i:i+2]
            tx1, tx2, tanline = _tan_data(region_points, firm1, firm2, maxdist)
            below_line = (
                (df['utm_north'] <= tanline(df['utm_east'])) &
                (df['utm_north'] >=
                 region_points.loc[[firm1, firm2], 'utm_north'].min())
            )
            between_points = (
                (df['utm_east'] >= tx1) & (df['utm_east'] <= tx2))
            in_hull = below_line & between_points
            in_region[in_hull.values] = True

    # Restrict
    regdata = regdata[in_region.values].copy()

    return regdata

def _push_out_southwest(df):
    df.loc[800335, :] -= 3000

def _tan_data(df, firm1, firm2, maxkm):
    r = maxkm * 1000
    x1, y1 = df.loc[firm1, UTM].tolist()
    x2, y2 = df.loc[firm2, UTM].tolist()
    # Slope of tangent line (when r is equal, slope if tan line is slope of
    # center line)
    m = (y2 - y1) / (x2 - x1)
    tx1 = __tan_x(m, x1, r)
    tx2 = __tan_x(m, x2, r)

    b = __tan_intercept(tx1, m, r, x1, y1)
    assert b == __tan_intercept(tx2, m, r, x2, y2)

    def theline(x):
        return m * x + b

    return tx1, tx2, theline

def __tan_x(m, x0, r):
    return - m * r / np.sqrt(1 + m ** 2) + x0

def __tan_intercept(tx, m, r, x, y):
    return __circle(tx, r, x, y) - m * tx

def __circle(x0, r, x, y):
    return np.sqrt(r ** 2 - (x0 - x) ** 2) + y


def prep_geounit(unit, **kwargs):
    if unit == 'sale':
        return prep_sale(**kwargs)
    elif unit == 'patzip':
        return prep_patzip(**kwargs)
    elif unit == 'bg':
        return prep_bg(**kwargs)


@load_or_build(data_path('regdata', 'reg_sale.p'))
def prep_sale():

    geounit_df = load_geounit('house')
    keep_house_vars = ['property_id', 'lnp', 'year', 'quarter', 'bg',
                       'lotsize', 'baths', 'beds', 'sqft', 'yr_blt',
                       'yr_blt_effect'] + UTM
    geounit_df = geounit_df[keep_house_vars]
    geounit_df = _sample_years(geounit_df)
    # Keep only sales
    geounit_df = geounit_df[geounit_df['lnp'].notnull()]
    # Merge in airq vars
    geounit_df = merge_airq(geounit_df, 'house')

    return geounit_df


@load_or_build(data_path('regdata', 'reg_patzip.p'))
def prep_patzip():

    geounit_df = load_geounit('patzip')
    # Drop if date is imprecise(?)
    geounit_df = geounit_df[geounit_df['quarter'].notnull()]
    geounit_df = _sample_years(geounit_df)
    # Drop utter garbage
    crap_vars = ['los', 'pay_cat', 'pay_type', 'pay_plan', 'charge']
    geounit_df.drop(crap_vars, axis=1, inplace=True)
    # Merge in UTM
    # TODO: use `util.buildblocks` here, not stored file that I just deleted.
    patzips_utm = pd.read_pickle(data_path('zips_pwt_utm.p'))
    geounit_df = geounit_df.join(patzips_utm, on='zip', how='inner')
    # Merge in population
    blocks = load_blocks_wzip()
    zip_pop = blocks.groupby('zip')['pop2000'].sum()
    geounit_df = geounit_df.join(zip_pop, on='zip')

    geounit_df = merge_airq(geounit_df, 'patzip')

    return geounit_df


@load_or_build(data_path('regdata', 'reg_bg.p'))
def prep_bg():

    # For now, 2000 only has pop
    geounit_df = load_bgdata(2005)
    geounit_df = geounit_df.append(load_bgdata(2000)).reset_index()

    # Create UTM, weighted aermod
    blocks = load_blocks().reset_index()
    blocks['bg'] = blocks['blockID'].str[:-3]

    for model in ('aermod', 'tria5'):
        airq = load_full_exposure('block', model)
        airq = swap_index(airq, blocks, UTM, 'blockID')
        airq = build_avg(airq, blocks, 'bg', 'blockID', weight_name='pop2000',
                         oth_index='quarter',
                         force_coverage=True)   # XXX temp patch

        # Merge in own-year
        airq_long = airq.stack('year').unstack('quarter')
        airq_long.columns = ['{}_q{}'.format(x[0], x[1])
                             for x in airq_long.columns]
        airq_long[model] = airq_long.filter(like=model + '_q').mean(axis=1)
        airq_long.reset_index(inplace=True)
        geounit_df = pd.merge(geounit_df, airq_long, on=['bg', 'year'],
                              how='left')

        # Merge in base-year
        airq_wide = airq.loc[:, pd.IndexSlice[IV_YEARS, :]].unstack('quarter')
        new_names = ['{}_{}q{}'.format(x[1], x[0], x[2])
                     for x in airq_wide.columns]
        airq_wide.columns = new_names
        geounit_df = pd.merge(geounit_df, airq_wide.reset_index(),
                              on='bg', how='left')

    # Get weighted UTM
    bg_weighted_utm = build_avg(blocks.set_index('blockID')[UTM], blocks,
                                'bg', 'blockID', weight_name='pop2000')
    bg_weighted_utm = bg_weighted_utm[UTM].fillna(0).copy()
    bg_weighted_utm = np.around(bg_weighted_utm).astype(int)
    geounit_df = geounit_df.join(bg_weighted_utm, on='bg')
    # Merge in pop2000
    geounit_df = geounit_df.join(blocks.groupby('bg')['pop2000'].sum(),
                                 on='bg')
    # Merge in hh2000
    hh2000 = geounit_df.loc[geounit_df['year'] == 2000, ['bg', 'households']]
    geounit_df = geounit_df.join(
        hh2000.set_index('bg').squeeze().to_frame('hh2000'),
        on='bg'
    )

    # Fill any remaining airq model missings
    geounit_df = geounit_df.fillna(0)

    return geounit_df


def _sample_years(df):
    df = df[(df['year'] >= 1995) & (df['year'] <= YEART)].copy()
    df['year'] = df['year'].astype(int)
    df['quarter'] = df['quarter'].astype(int)
    return df


def merge_airq(geounit_df, geounit):
    # for model in ('aermod',):
    for model in ('aermod', 'tria5', 'unif2'):
        # Main, current-year var
        airq = load_full_exposure(geounit, model)
        airq_long = airq.stack('year').reset_index()
        time_merge = ['year', 'quarter']
        geounit_df = pd.merge(geounit_df, airq_long, on=UTM + time_merge,
                              how='left')
        geounit_df[model].fillna(0, inplace=True)
        # Base-year var
        airq_wide = airq.loc[:, pd.IndexSlice[IV_YEARS, :]].unstack('quarter')
        new_names = ['{}_{}q{}'.format(x[1], x[0], x[2])
                     for x in airq_wide.columns]
        airq_wide.columns = new_names
        geounit_df = pd.merge(geounit_df, airq_wide.reset_index(), on=UTM,
                              how='left')
        for col in new_names:
            geounit_df[col].fillna(0, inplace=True)

    return geounit_df


@load_or_build(data_path('tmp_firms_affecting_region.pkl'))
def firms_affecting_region(maxdist=SAMPLE_REGION_DIST, region='sw'):
    """ Firms affecting houses in sample region. """
    df = regionspec(maxdist, region=region)
    firm_utm = load_stacks().set_index('facid')[UTM].drop_duplicates()
    house_utm = df[UTM].drop_duplicates()
    used_firms = center_data(firm_utm, house_utm, maxdist=MAXDIST)
    return used_firms


# Methods that have to be here to use `center_sw`
@load_or_build(data_path('{}s_rawfirm_{}.p'), path_args=[0, 1])
def all_raw_exposure(geounit, model):
    """ DataFrame of *all* raw expsoure, at once """
    # NOTE: This should be in `atmods.io`, BUT without restricting data with
    # `center_sw`, it is way too big. So this has to be here to avoid circular
    # imports

    firm_list = _get_all_facids(geounit, model)
    # Initialize output matrix
    utms = center_sw(load_geounit(geounit)[UTM].drop_duplicates())
    utm_idx = utms.set_index(UTM).index
    pn = pd.Panel(np.zeros((len(firm_list), len(utms), 4)),
                  items=firm_list,
                  major_axis=utm_idx,
                  minor_axis=range(1, 4 + 1),
                  )
    pn.minor_axis.name = 'quarter'
    for facid in firm_list:
        print "Facid: {}".format(facid)
        this_firms = load_firm_normed_exp(geounit, model, facid)
        pn[facid].update(this_firms)
        del this_firms

    df = pn.to_frame().astype(np.float32)   # Cast to float just in case

    return df


def load_full_exposure(geounit, model, **kwargs):
    """
    Wrapper around `atmods.io.load_or_build` to be used with higher-level
    methods that access both kriged and non-kriged exposure data. This is
    necessary because the monitor sample used in kriging is defined using
    `center_sw`, so `load_kriged` cannot be any lower in the package hierarchy
    than this file.
    """
    if 'krige' in model:
        return load_kriged(geounit, model, **kwargs)
    else:
        return aio_load_full_exposure(geounit, model, **kwargs)


# Kriging
def check_variogram_fit():
    """
    Used (interactively) to assess the fit of the variogram used in
    `load_kriging`.
    """
    mons = _def_monitor_samp(1999, 4)
    X = mons[UTM].values
    y = mons['nox'].values
    return check_variogram(X, y, mle_args=dict(param0=[4274, 4]), scat=True)


@load_or_build(data_path('{}s_{}_{}q{}.p'),
               path_args=[0, 1, 'mon_year', 'mon_quarter'])
def load_kriged(geounit, model, mon_year=1999, mon_quarter=4, _ret_wt=False):
    """
    A Series of Kriged values for `geounit` for a single year and quarter.
    """
    mons = _get_mon_samp_w_aermod(mon_year, mon_quarter)
    X = mons[UTM].values
    y = mons['nox'].values      # Specify krig params using NOx in a single
                                # year/quarter
    geounit = load_geounit('grid')
    X0 = geounit[UTM] / 1000
    weights = kriging_weights(X, y, X0, mle_args=dict(param0=[4274, 4]))
    # Use weights to get Kriged values
    reading_name = model.replace('krige-', '')
    assert reading_name in ('aermod', 'nox', 'ozone')
    reading = mons[reading_name].values
    outdf = pd.DataFrame(weights.dot(reading).squeeze(),
                         index=geounit[UTM].set_index(UTM).index)
    outdf.columns = pd.MultiIndex.from_arrays([[mon_year], ['krige-aermod']])
    if _ret_wt:
        return outdf, weights
    else:
        return outdf

def _get_mon_samp_w_aermod(year, quarter):
    aermod = load_full_exposure('monitor', 'aermod').sort_index()
    aermod = aermod.loc[pd.IndexSlice[:, :, quarter], year].squeeze()
    aermod.reset_index('quarter', drop=True, inplace=True)
    mons = _def_monitor_samp(year, quarter)
    mons_utm = (mons[UTM] * 1000).astype(int).set_index(UTM).index
    aermod_small = aermod.reindex(mons_utm)
    mons['aermod'] = aermod_small.values
    return mons

def _def_monitor_samp(year, quarter):
    df = load_monitors().set_index('site')
    df = center_sw(df, maxdist=30)      # Restrict which monitors are used
    df = df[(df['year'] == year) & (df['quarter'] == quarter)]
    df['nox'] *= 1000
    assert df.index.is_unique
    df[UTM] /= 1000     # utm in km
    return df


if __name__ == '__main__':
    df = all_raw_exposure('house', 'aermod')
