from __future__ import division

import os.path as path
import re
import multiprocessing as mp

from econtools import (int2base, base2int, load_or_build, load_or_build_direct,
                       force_iterable, generate_chunks,)

import pandas as pd
import numpy as np

from util import UTM
from util.system import hostname, data_path, BULK_DATA
from clean import (load_geounit, load_blocks_wzip, load_monitors,
                   load_houses_utm)
from clean.fake_grid import round_nearest, GRID_SIZE
from clean.pr2 import load_stacks, emissions, elec_facids
from clean.pr3.toxics import load_named_toxic_emissions
from atmods.env import FIRMS_FOR_ALTMAXDIST
from atmods.kernels import KERNELS
from atmods.chunktools import calc_aermod_units
from atmods.interpolate import interpolate

AIRQ_PATH_STEM = 'airq'
AIRQ_PATH = data_path(AIRQ_PATH_STEM)
# Chunked files only live in one of these folders (depends on system)
LOCAL_CHUNK_PATH = path.join(AIRQ_PATH, 'from_regal')
BULK_CHUNK_PATH = path.join(BULK_DATA, 'airq')

CHUNK_ID_BASE = 62
# `pr2` nox is not a balanced panel w/ nan's, so can't pull years from firms
PR2_YEARS = range(1994, 2006 + 1)


# IO convenience methods
def normed_firmexp_path(geounit, model, facid, chunk_info=None,
                        altmaxdist=False):
    """
    Add full path (dir and extension) to output from `normed_firmexp_fname`.
    """
    _check_valid_args(geounit, model)
    folder = _normed_firmexp_folder(chunk_info=chunk_info,
                                    altmaxdist=altmaxdist)
    if type(chunk_info) is bool:
        chunk_info = None           # If `chunk_info` isn't a tuple with actual
                                    # chunk info, then we just wanted to
                                    # be in the right folder, e.g., for
                                    # use with `glob`

    filename = normed_firmexp_fname(geounit, model, facid,
                                    chunk_info=chunk_info) + '.p'
    full_path = path.join(folder, filename)

    return full_path

def _normed_firmexp_folder(chunk_info=None, altmaxdist=False):
    if chunk_info and hostname == 'harvard':
        # For pulling from regal on cluster
        folder = BULK_CHUNK_PATH
    elif chunk_info and hostname == 'mine':
        # When data is on local machine
        folder = LOCAL_CHUNK_PATH
    elif chunk_info:
        raise ValueError
    else:
        folder = AIRQ_PATH

    if altmaxdist:
        folder = folder.replace(AIRQ_PATH_STEM, AIRQ_PATH_STEM + '30')

    return folder


def normed_firmexp_fname(geounit, model, facid, chunk_info=None):
    """
    Generate unique filename for geounit, exposure model, and firm(chunk).
    """

    ONECHAR_KERNEL = {'unif': 'u', 'tria': 't', 'triw': 'w', 'epan': 'e'}
    RAW_FIRMS_FILESTEM = '{geounit_init}{modelname}{facid}'
    CHUNK_SUFFIX = 'c{chunk_id}{num_chunks}'

    _check_valid_args(geounit, model)

    # Make 'modelname' for filename
    if model == 'aermod':
        modelname = 'A'
    else:
        kern, bandwidth = parse_kernmodel(model)
        modelname = (ONECHAR_KERNEL[kern].upper() +
                     str(bandwidth) + 'f')    # 'f' to split band and facid

    geounit_init = geounit[0]
    filename = RAW_FIRMS_FILESTEM.format(geounit_init=geounit_init,
                                         modelname=modelname,
                                         facid=facid)

    # Add chunk_info if needed
    if chunk_info:
        assert len(chunk_info) == 2
        # Convert chunk numbers to high base so only need one character
        chunk_id_newbase = int2base(chunk_info[0], CHUNK_ID_BASE)
        num_chunks_newbase = int2base(chunk_info[1], CHUNK_ID_BASE)
        chunk_suffix = CHUNK_SUFFIX.format(chunk_id=chunk_id_newbase,
                                           num_chunks=num_chunks_newbase)
        filename += chunk_suffix

    return filename


def parse_firm_info(filename):
    """
    Extract facid and chunk_info from `filename` generated by
    `normed_firmexp_fname`.
    """

    # Last char before facid should be 'f' or upper case (model type), then
    # facid, "c", chunk_id,  total_chunks (chunks are base `CHUNK_ID_BASE`)
    chunk_info_re = r'[fA-Z](\d+)c([0-9A-Za-z])([0-9A-Za-z])\.p'
    facid, raw_cid, raw_cmax = re.search(chunk_info_re, filename).groups()
    facid = int(facid)

    chunk_id = base2int(raw_cid, CHUNK_ID_BASE)
    num_chunks = base2int(raw_cmax, CHUNK_ID_BASE)

    return facid, chunk_id, num_chunks


def filepath_airqdata(geounit, model, elec=None):
    filename = "{geounit}s_{model}".format(geounit=geounit, model=model)
    # Add electric suffix if necessary
    if elec is not None:
        try:
            assert elec is not True and elec in (0, 1, '{}')
        except AssertionError:
            raise ValueError("`elec` must be 0 or 1.")
        filename += '_el{}'.format(elec)
    filename += '.pkl'
    full_path = data_path(filename)
    return full_path


def parse_kernmodel(kernmodel):
    """
    Separate kernel's model name into kernel and bandwidth.

    Ex: `tria5` is triangle, 5-km bandwidth.
    """
    kern, bandwidth = kernmodel[:-1], kernmodel[-1]

    try:
        assert kern in KERNELS
    except AssertionError:
        raise ValueError

    bandwidth = int(bandwidth)

    return kern, bandwidth


def _check_valid_args(geounit, model):
    supported_geounits = ('house', 'block', 'patzip', 'monitor', 'grid')
    supported_models = ('aermod', 'interp', 'nox', 'ozone') + KERNELS

    if geounit not in supported_geounits:
        raise ValueError("Geo-Unit '{}' not supported.".format(geounit))

    if model not in supported_models:
        try:
            kern, band = parse_kernmodel(model)
        except ValueError:
            raise ValueError("Model '{}' not valid.".format(model))


# Main 'get' methods
def load_full_exposure(geounit, model, **kwargs):

    if geounit == 'patzip':
        df = load_patzip_full_exposure(model, **kwargs)
    elif model in ('interp', 'nox', 'ozone'):
        df = load_interp(geounit, model, **kwargs)
    else:
        df = _load_full_exposure_guts(geounit, model, **kwargs)

    # XXX temp patch to eliminate straggler levels (former multi-level got
    # smashed to tuples somewhere; put back to multi-level)
    df.columns = pd.MultiIndex.from_tuples(df.columns.values,
                                           names=df.columns.names)

    return df


@load_or_build(filepath_airqdata('{}', '{}', elec='{}'), path_args=[0, 1, 2])
def load_electric_exposure(geounit, model, elec):
    """
    Return std exposure frame, conditional on electric status.

    The 'electrics' file is build up from firm files. The 'non-electric' file
    is the full sample minus electrics.
    """

    electrics = elec_facids().index
    if elec == 1:
        df = sum_allfirms_exposure_mp(geounit, model, firm_list=electrics)
    elif elec == 0:
        full = load_full_exposure(geounit, model)
        elecs = load_electric_exposure(geounit, model, 1)
        df = full.sub(elecs, fill_value=0)
    else:
        raise ValueError("`elec` must be 0 or 1.")

    return df


@load_or_build(data_path('{}s_interp{}_{}-{}_{}.p'),
               path_args=[0, 'radius', 'year0', 'yearT', 1])
def load_interp(geounit, model, year0=1997, yearT=2005, radius=15):
    """
    Load values of `model` for `geounit` that have been interpolated from
    actual monitor locations using inverse distance weighting.

    NOTE: `model` value `interp` is the aermod value at the monitor locations.
      This naming is used to avoid ambiguities where `model='aermod'` refers to
      the geounit's own aermod values.
    """
    # Load geounit's UTM
    target_utm = load_geounit(geounit)[UTM].drop_duplicates()

    # Load interpolation source UTM, time, and value as Series
    if model == 'interp':
        source_df = load_full_exposure('monitor', 'aermod')
        source_df = source_df.stack('year').squeeze()
    elif model in ('nox', 'ozone'):
        timevars = ['year', 'quarter']
        source_df = load_monitors()[UTM + timevars + [model]]
        source_df = source_df.set_index(UTM + timevars).squeeze()
    else:
        raise NotImplementedError

    # Rough restriction of monitor sample to save RAM
    source_north = source_df.reset_index()['utm_north']
    north_max = source_north.max()
    source_df = source_df[(source_north <= north_max + 20000).values]

    # Restrict to full-coverage monitors in (year0, yearT)
    if model == 'interp':
        good_monitors = load_monitors(year0=year0, yearT=yearT)
        good_monitors = good_monitors.drop_duplicates(UTM).set_index(UTM)
        good_utms = pd.Series(np.ones(len(good_monitors)),
                              index=good_monitors.index,
                              name='_covered')
        timevars = ['year', 'quarter']
        source_df = source_df.unstack(timevars)
        is_covered = source_df.join(good_utms)['_covered'] == 1
        source_df = source_df[is_covered.values].stack(timevars)
    else:
        # Already restricted
        pass

    cv = geounit == 'monitor'
    interp_df = interpolate(source_df, target_utm, cv=cv, cutoff_km=radius)

    interp_df = interp_df.stack(['year', 'quarter'])
    interp_df = interp_df.to_frame(model)
    interp_df.columns.name = 'model'
    interp_df = interp_df.unstack('year')

    interp_df.columns = interp_df.columns.reorder_levels(['year', 'model'])

    return interp_df


@load_or_build(filepath_airqdata('patzip', '{}'), path_args=[0])
def load_patzip_full_exposure(model):
    df = build_zip_from_block(model)
    return df


def _load_full_exposure_guts(geounit, model, use_grids=False, use_mp=False,
                             **kwargs):
    """ Build `geounit`s Aermod or kernel exposure (`model`) from all firms """

    if geounit != 'grid' and use_grids:
        buildfunc = pull_geounit_exposure_from_grids
    elif use_mp:
        buildfunc = sum_allfirms_exposure_mp
    else:
        buildfunc = sum_allfirms_exposure

    @load_or_build(filepath_airqdata(geounit, model))
    def build_full_exposure(*args):
        return buildfunc(*args)

    return build_full_exposure(geounit, model, **kwargs)


def pull_geounit_exposure_from_grids(geounit, model, **kwargs):
    """
    The 'grid' geounit is a superset of all other geounits. This function
    creates data for `geounit` from the grid's data to avoid using
    `sum_allfirms_exposure` directly since it takes forever.
    """
    grids, grids_orig_cols = _prep_grids_df(model)
    geounits_utm_idx = _geounits_utm_idx(geounit)
    geounits_model = grids.join(geounits_utm_idx, how='inner')
    # Non-house geounits don't necessarily have rounded UTM values. Their
    # 'real' UTM's are merged in as columns, now replace rounded UTM in index
    # with real values.
    if geounit != 'house':
        for utm in UTM:
            geounits_model.rename(columns={'{}_real'.format(utm): utm},
                                  inplace=True)
        geounits_model.set_index(UTM, inplace=True)
    # Put quarter back in index (was moved out by `_prep_grids_df`
    geounits_model.set_index('quarter', append=True, inplace=True)
    geounits_model.sort_index(inplace=True)
    geounits_model.columns = grids_orig_cols
    return geounits_model

def _prep_grids_df(model):
    grids = load_full_exposure('grid', model)
    grids_orig_cols = grids.columns     # Merge/join wrecks multicolumns, save
                                        # these for after the merge
    grids.columns = grids.columns.droplevel('model')
    grids.reset_index('quarter', inplace=True)  # Want to merge on UTM only
    return grids, grids_orig_cols

def _geounits_utm_idx(geounit):
    if geounit == 'monitor':
        df = load_monitors(fullcover=False)
    else:
        df = load_geounit(geounit)
    df = df[UTM].drop_duplicates()
    # Non-house geounits don't necessarily have rounded UTM values.
    if geounit != 'house':
        for utm in UTM:
            df['{}_real'.format(utm)] = df[utm]
            df[utm] = df[utm].apply(round_nearest, args=(GRID_SIZE,))
    utm_idx = df.set_index(UTM)
    return utm_idx


# Building/aux methods
def sum_allfirms_exposure_mp(geounit, model, firm_list=None):
    """ Multi-processing version of `sum_allfirms_exposure` """
    CORES = 2 if geounit == 'grid' else 4   # 'grid' uses a lot of RAM
    # Break `firm_list` into number of cores
    if firm_list is None:
        firm_list = _get_all_facids(geounit, model)
    list_chunk_size = int(np.ceil(len(firm_list) / CORES))
    firm_list_chunks = list(generate_chunks(firm_list, list_chunk_size))
    # Get the jobs going
    output = mp.Queue()
    processes = [
        mp.Process(
            target=_mp_wrapper,
            args=(geounit, model, this_firm_list, output)
        )
        for this_firm_list in firm_list_chunks
    ]
    print "Starting {} processes!".format(len(processes))
    for p in processes:
        p.start()
    for p in processes:
        p.join(1)

    results = [output.get() for p in processes]
    full_scaled = results.pop()
    while results:
        full_scaled = full_scaled.add(results.pop(), fill_value=0)

    return full_scaled

def _mp_wrapper(geounit, model, this_firm_list, q):
    result = sum_allfirms_exposure(geounit, model, this_firm_list)
    q.put(result)

def _get_all_facids(geounit, model):
    if model == 'aermod':
        units = calc_aermod_units(geounit)
        units = units[units > 0]
        firm_list = units.index.unique().tolist()
    else:
        firm_list = load_stacks()['facid'].unique()

    return firm_list


def sum_allfirms_exposure(geounit, model, firm_list=None):
    """ Sum exposure across all firms """

    if firm_list is None:
        firm_list = _get_all_facids(geounit, model)
    else:
        firm_list = force_iterable(firm_list)

    allfirms_emit_gs = formatted_firms_emission_grams_sec(model=model)

    running_tot = _prep_allfirms_exp_df(geounit, model)
    for fid in firm_list:
        print str(fid)
        firms_scaled = load_firm_exposure(geounit, model, fid,
                                          allfirms_emit_gs=allfirms_emit_gs,
                                          )
        if firms_scaled is None:
            continue
        temp_df = firms_scaled.reindex(index=running_tot.index).fillna(0)
        running_tot += temp_df
        del firms_scaled, temp_df

    return running_tot

def _prep_allfirms_exp_df(geounit, model):
    # Get UTM index
    utm = load_geounit(geounit)[UTM].drop_duplicates()
    utm_idx = utm.astype(np.int32).set_index(UTM).index

    idx_df = pd.DataFrame(index=utm_idx)
    if 'aermod' in model and model != 'aermod_nox':
        idx = idx_df.index
    else:
        for q in range(1, 4 + 1):
            idx_df[q] = 0
        idx_df.columns.name = 'quarter'
        idx = idx_df.stack('quarter').index

    # Add years wide
    col_idx = exposure_df_column_idx(PR2_YEARS, model)
    df_shape = (len(idx), len(col_idx))
    df = pd.DataFrame(np.zeros(df_shape), index=idx, columns=col_idx)

    return df


def load_firm_exposure(geounit, model, facid, allfirms_emit_gs=None,
                       altmaxdist=None):
    """
    Scale up raw exposure by actual emissions.

    `allfirms_noxgs` -- available for quicker looping over many firms
        without reading the emissions data fresh for every firm.
    `altmaxdist` -- switch for which Aermod radius to use for `facid`. `None`
      is default and defers to list in `atmods.env`.
    """

    # Load this firm's nox in g/s
    if allfirms_emit_gs is None:
        firms_emit_gs = formatted_firms_emission_grams_sec(facid=facid,
                                                           model=model)
    else:
        if model == 'aermod':
            firms_emit_gs = allfirms_emit_gs.loc[facid]
        elif 'aermod' in model:
            try:
                firms_emit_gs = allfirms_emit_gs.loc[facid]
            except:
                return None

    # Load raw air quality exposure data
    normed_model = 'aermod' if 'aermod' in model else model
    normed_exp = load_firm_normed_exp(geounit, normed_model, facid,
                                      altmaxdist=altmaxdist).sort_index()

    if model == 'aermod_nox':
        actual_exp = _firms_aermod_exp(normed_exp, firms_emit_gs, model)
    elif 'aermod' in model:
        actual_exp = _firms_aermod_not_nox_exp(normed_exp, firms_emit_gs,
                                               model)
    else:
        actual_exp = _firms_kernel_exp(normed_exp, firms_emit_gs, model)

    # If a firm has missing emissions in a year, assume 0
    actual_exp = actual_exp.fillna(0)

    return actual_exp


def _firms_aermod_exp(normed_exp, firms_noxgs, model):
    """ Outer product of normed exp and actual emissions for each quarter. """
    columns = exposure_df_column_idx(firms_noxgs.index, model)
    pn = pd.Panel(np.zeros((4, len(normed_exp), len(columns))),
                  items=range(1, 4 + 1),
                  major_axis=normed_exp.index,
                  minor_axis=columns)
    pn.items.name = 'quarter'
    for q in xrange(1, 4+1):
        pn[q].update(_cross_df(normed_exp[q], firms_noxgs[q], columns))

    actual_exp = pn.transpose(2, 1, 0).to_frame()

    # Round to 5 decimals (AERMOD's actual limit)
    actual_exp = np.around(actual_exp * 1e5) / 1e5

    return actual_exp


def _firms_aermod_not_nox_exp(normed_exp, firms_noxgs, model):
    """ Outer product of normed exp and actual emissions for each quarter. """
    columns = exposure_df_column_idx(firms_noxgs.index, model)
    # Non-NOx AERMOD is annual emissions
    annual_normed_exp = normed_exp.mean(axis=1)
    actual_exp = _cross_df(annual_normed_exp, firms_noxgs, columns)

    # Round to 5 decimals (AERMOD's actual limit)
    actual_exp = np.around(actual_exp * 1e5) / 1e5

    return actual_exp


def _firms_kernel_exp(normed_exp, firms_noxgs, model):
    """ Outer product of normed exp and actual emissions for each quarter.  """
    # Kernels don't vary by quarter, only firm emissions do,
    # so do quarters all at once
    long_noxgs = firms_noxgs.stack('quarter', dropna=False)
    wide_scaled = _cross_df(normed_exp, long_noxgs, long_noxgs.index)
    # Reshape to (UTM, quarter) x (year)
    actual_exp = wide_scaled.stack('quarter').sort_index(axis=1)
    actual_exp.columns = exposure_df_column_idx(actual_exp.columns, model)

    # This is the correct way, but still needs ad hoc adjustment...
    if 0 == 1:
        # Rescale to average daily ug/m^3
        actual_exp *= (2000 *           # Lbs/ton
                       453.59237 *      # Grams/lbs
                       1e6 /            # Micrograms / gram
                       90)              # Days / quarter
        actual_exp /= 1e6   # Still way too big, make 'cylindar' 1000 km tall
    # ...so just do ad hoc to begin with
    else:
        actual_exp *= 1e7

    return actual_exp


def _cross_df(raw, emit, columns):
    """ Outer product of normed exposure and emissions """
    arr = np.outer(raw, emit)
    scaled = pd.DataFrame(arr, index=raw.index, columns=columns)
    return scaled


def formatted_firms_emission_grams_sec(facid=None, model='aermod_nox'):
    # Get correct emissions data
    print(model)
    aermod_not_nox = 'aermod' in model and model != 'aermod_nox'
    if aermod_not_nox:
        print("aermod not nox")
        emit = load_named_toxic_emissions(name=model.replace('aermod_', ''))
    else:
        print("std aermod")
        emit = emissions()

    if facid:
        emit = emit.loc[facid]

    # NOx data are quarterly, toxics are annual
    if aermod_not_nox:
        pass
    else:
        emit = emit.unstack('quarter')

    # Some firms don't have all years (or quarters). Fill with nan
    if facid:
        emit = emit.reindex(PR2_YEARS)
        if not aermod_not_nox:
            emit = emit.reindex(columns=range(1, 4 + 1))
    elif aermod_not_nox:
        emit = (emit
                .unstack('year')
                .reindex(columns=PR2_YEARS)
                .stack('year', dropna=False))
    else:
        emit = emit.to_panel().to_frame(filter_observations=False)

    # Criteria pollutants measured tons/year, toxics lbs/year
    criteria_pollutant = (
        'aermod' in model and
        model.split('_')[1] in ('nox', 'co', 'rog', 'sox', 'tsp'))
    if criteria_pollutant:
        firms_emit_gs = _tonsperyq_to_gramspersec(emit)
    else:
        firms_emit_gs = _lbs_per_yr_to_grams_per_sec(emit)

    return firms_emit_gs

def _tonsperyq_to_gramspersec(tons_nox, quarterly=True):
    """Convert tons per year (or quarter) to grams per second."""
    tpy_to_gps = (2000. * 453.59237             # Lbs/ton * grams/lbs
                  ) / (365.25 * 24 * 60 * 60)   # seconds/year
    year_units = 4 if quarterly else 1          # Quarters/year (for Q-ly data)
    nox_gs = tons_nox * tpy_to_gps * year_units

    return nox_gs

def _lbs_per_yr_to_grams_per_sec(lbs_emit):
    ppy_to_gps = (453.59237 /               # grams/lbs
                  (365.25 * 24 * 60 * 60))  # seconds/year
    emit_gs = lbs_emit * ppy_to_gps
    return emit_gs



def load_firm_normed_exp(geounit, model, facid, altmaxdist=None, **lobkwargs):
    """
    Load unscaled exposure data for a single firm.
    Aermod in N x 4 (quarters) DataFrame, kernels in N x 0 Series.

    `altmaxdist` is switch for which Aermod radius to use for `facid`. `None`
      is default and defers to list in `atmods.env`.
    """

    # Use `altmaxdist` for selected firms by default
    if altmaxdist is None:
        altmaxdist = facid in FIRMS_FOR_ALTMAXDIST

    # Independent 'house_aermod_FIRM' files are no longer used
    if geounit == 'house' and model == 'aermod':
        df = _get_houses_rawexp_from_grids(facid, altmaxdist=altmaxdist,
                                           **lobkwargs)
    else:
        filepath = normed_firmexp_path(geounit, model, facid,
                                       altmaxdist=altmaxdist)
        if path.isfile(filepath):
            df = pd.read_pickle(filepath)
        else:
            errstr = (
                "File for normed firm exposure\n{}\ndoes not exist."
                "Run `batchrun.py` or `run_and_write.py`"
            )
            raise IOError(errstr.format(filepath))

    if df.shape[1] == 1:
        # If shape[1] == 1, then this is a kernel or Aermod in long format.
        # Convert to Series (`squeeze` will turn 1-row DF into float)
        df = df.iloc[:, 0]
    if 'quarter' in df.index.names:
        # NOTE: Aermod (via `run_and_write._format_aermodout`) should output
        # data wide (UTM x quarter). But in the case of legacy files, reshape
        # it here.
        df = df.unstack('quarter')
    return df


def _get_houses_rawexp_from_grids(facid, altmaxdist=False, houses_utm=None,
                                  _load=True, _rebuild=False):
    """ `altmaxdist` is switch for which radius to use for `facid` """
    if _load:
        filepath = normed_firmexp_path('house', 'aermod', facid,
                                       altmaxdist=altmaxdist)
        df = load_or_build_direct(filepath,
                                  build=_get_houses_rawexp_from_grids,
                                  force=_rebuild,
                                  bargs=(facid,),
                                  bkwargs=dict(houses_utm=houses_utm,
                                               altmaxdist=altmaxdist,
                                               _load=False),
                                  )
        return df

    if houses_utm is None:
        houses_utm = _prep_houses_utm()
    grids = load_firm_normed_exp('grid', 'aermod', facid)
    houses = grids.join(houses_utm, how='inner')
    return houses

def _prep_houses_utm():
    houses_utm = load_houses_utm().set_index(UTM)
    return houses_utm


def build_zip_from_block(model, block_airq=None):
    zip_utm = load_blocks_wzip()
    zip_utm = zip_utm[zip_utm['zip'].notnull()]     # Drop anyone w/o a zip
    # Load and tweak blocks' airq data
    if block_airq is None:
        fullsamp = True
        block_airq = load_full_exposure('block', model)
    else:
        fullsamp = False
    block_airq = block_airq.reset_index()
    block_airq.columns = block_airq.columns.droplevel('model')
    # Merge
    block_wz = pd.merge(block_airq, zip_utm, on=UTM, how='inner')  # Drop w/o z
    # Weighted average
    zip_airq = block_wz.groupby(['zip', 'quarter']).apply(
        lambda x: pd.Series(np.average(x, weights=x['pop2000'], axis=0),
                            index=x.columns))

    #   Round UTM
    zip_airq[UTM] = np.around(zip_airq[UTM]).astype(int)
    #   To merge with raw patzip data
    # TODO This only requires block data, should be done earlier?
    # XXX THIS IS VERY BAD
    if fullsamp:
        _save_zips_utm(zip_airq)

    # Must conform to UTM-index pattern
    zip_airq.drop(['quarter', 'zip', 'pop2000'], axis=1, inplace=True)
    zip_airq = zip_airq.reset_index('zip', drop=True)
    zip_airq = zip_airq.reset_index()
    zip_airq = zip_airq.drop_duplicates()  # Don't know how, but 5 dups
    zip_airq = zip_airq.set_index(UTM + ['quarter'])
    # Re-build the two-level columns
    zip_airq.columns = exposure_df_column_idx(zip_airq.columns, model)

    return zip_airq

def _save_zips_utm(df):
    """Save zips' pop-weighted UTM"""
    zips_utm = df[UTM + ['zip']].drop_duplicates().set_index('zip')
    zips_path = data_path('zips_pwt_utm.p')
    zips_utm.to_pickle(zips_path)


def exposure_df_column_idx(years, model):
    """ Create columns (`year`, `model`) with model constant. """
    col_idx = pd.MultiIndex.from_tuples(
        [(int(x), model) for x in years],
        names=['year', 'model']
    )
    return col_idx


if __name__ == "__main__":
    pass
