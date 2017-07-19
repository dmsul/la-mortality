from __future__ import division

import os

import pandas as pd
import numpy as np

from econtools import force_iterable

from util import UTM
from util.distance import center_data
from clean import load_geounit
from clean.pr2 import load_stacks
from atmods.io import normed_firmexp_path, parse_kernmodel
from atmods.aermod import Aermod
from atmods.env import MAXDIST, ALTMAXDIST
from atmods.wrapperargs import ModelArgs
from atmods.kernels import polar_kernel


def run_and_write(geounit, model, facid, chunk_info=None, overwrite=False,
                  altmaxdist=True,
                  quiet=False, save=True):

    # Load receptor data
    receptorDF = load_geounit(geounit)
    # Restrict columns for memory efficiency
    grab = None     # XXX Not sure if I'll want to re-implement, hold for now
    if grab:
        receptorDF = receptorDF[UTM + [grab]].drop_duplicates()
    else:
        receptorDF = receptorDF[UTM].drop_duplicates()

    if model == 'aermod':
        sourceDF = load_chunked_stacks(facid, chunk_info)
        run_a_firm(geounit, model, facid, receptorDF, sourceDF, grab,
                   chunk_info=chunk_info, overwrite=overwrite, quiet=quiet,
                   save=save, altmaxdist=altmaxdist)
    else:
        stacks = load_stacks()

        if facid is None:
            facid = tuple(stacks['facid'].unique())
        else:
            facid = force_iterable(facid)

        for fid in facid:
            sourceDF = stacks[stacks['facid'] == fid]
            run_a_firm(geounit, model, fid, receptorDF, sourceDF, grab,
                       overwrite=overwrite)

def load_chunked_stacks(facid, chunk_info):
    """
    Return the chunk of `facid`s stacks corresponding to `chunk_info`.
    `chunk_info` is (`chunk_id`, `num_chunks`).
    """

    firms_stacks = load_stacks(facid)

    if chunk_info is None:
        return firms_stacks

    chunk_id, num_chunks = chunk_info

    if num_chunks in (1, None):
        return firms_stacks

    try:
        assert chunk_id <= num_chunks
    except AssertionError:
        err_str = "Chunk's id {} is out of bounds for num of chunks {}"
        raise ValueError(err_str.format(chunk_id, num_chunks))

    N = firms_stacks.shape[0]
    even_num_per_group = N // num_chunks
    remainder = N % num_chunks
    group_sizes = np.ones(num_chunks, dtype=int) * even_num_per_group
    # Distribute remainder evenly
    group_sizes[:remainder] += 1
    # Make array of stacks' chunk_ids
    stacks_id = np.array([
        idx + 1     # +1 so chunks are 1-indexed, not 0
        for idx, group_size in enumerate(group_sizes)
        for x in range(group_size)  # Repeat `idx` `group_size` times
    ])

    # Check that result makes sense
    assert sum(group_sizes) == N        # Every stack is allocated to one group
    assert len(stacks_id) == N          # stacks' ids align with stackDF
    assert max(stacks_id) == num_chunks    # Preserved num of chunks

    return firms_stacks[stacks_id == chunk_id]


def run_a_firm(geounit, model, facid, receptorDF, sourceDF, grab,
               altmaxdist=False,
               chunk_info=None, overwrite=False, quiet=False, save=True):

    file_path = normed_firmexp_path(geounit, model, facid,
                                    chunk_info=chunk_info,
                                    altmaxdist=altmaxdist)

    # Check if already done
    if os.path.isfile(file_path) and not overwrite:
        print "Skip Firm {}, file {} exists.".format(facid, file_path)
        return None

    # Center receptors around source
    maxdist = ALTMAXDIST if altmaxdist else MAXDIST
    # XXX: just increase the radius for now instead of grabbing
    if geounit == 'block':
        maxdist += 5
    center_firm_utm = load_stacks(facid)[UTM].mean()
    receptorDF = center_data(receptorDF, center_firm_utm, maxdist, grab=grab)
    receptorDF = receptorDF[UTM].drop_duplicates()
    # Run the model
    rawexposure = drive_model(receptorDF, sourceDF, model, quiet=quiet)
    # Write to disk
    if save:
        print "Writing {} for Firm {}".format(model, facid)
        rawexposure.to_pickle(file_path)

    return rawexposure


def drive_model(receptorDF, sourceDF, model, quiet=False):

    # Call model
    if model == 'aermod':
        rawexposure = Aermod(receptorDF, sourceDF, quiet=quiet).runModel()
        rawexposure = _format_aermodout(rawexposure)
    else:
        # Handle kernel info
        kern, bandwidth = parse_kernmodel(model)
        center = sourceDF[UTM].mean().tolist()
        receptorDF[model] = polar_kernel(receptorDF, h=bandwidth*1000.,
                                         kernname=kern, center=center)
        rawexposure = receptorDF.set_index(UTM)

        # Drop zero'd rows for kernels
        rawexposure = rawexposure[rawexposure.max(axis=1) > 0]

    return rawexposure

def _format_aermodout(rawexposure):
    """Expect rawexposure to have columns UTM, exposure, month"""
    df = rawexposure
    # UTM read from aermod as float, change to int
    df[UTM] = df[UTM].astype(np.int32)
    # Single precision only for exposure
    df['exposure'] = df['exposure'].astype(np.float32)
    # Collapse to quarter
    df['quarter'] = (((df['month'] - 1) // 3) + 1).astype(np.int8)
    collapsed = df.groupby(UTM + ['quarter'])['exposure'].mean()
    # Make data wide, (UTM x Quarter)
    collapsed = collapsed.squeeze().unstack('quarter')

    return collapsed


def fake_aermod(sources_receptors):
    I = sources_receptors.shape[0]
    rawexposure = pd.DataFrame(np.arange(I * 12).reshape(-1, 12),
                               columns=[range(1, 13), 12 * ['exposure']],
                               dtype=np.float32)
    rawexposure.columns.names = ['month', 'var']
    rawexposure.index = pd.MultiIndex.from_arrays(
        [sources_receptors['utm_east'], sources_receptors['utm_north']])
    rawexposure = rawexposure.stack(0)
    rawexposure.reset_index(inplace=True)
    return rawexposure


if __name__ == '__main__':
    # Get command line args
    args = ModelArgs()
    run_and_write(**args)
