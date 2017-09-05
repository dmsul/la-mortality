import subprocess
import StringIO
from os import path
import argparse

import pandas as pd

from econtools import generate_chunks

from util.system import hostname
from atmods.io import normed_firmexp_path, normed_firmexp_fname
from atmods.chunktools import calc_aermod_units, calc_resources


def pretty_out(geounit, model, altmaxdist=False):
    jobs_needed = jobs_to_run(geounit, model, altmaxdist=altmaxdist)
    padded = [jobname.ljust(8) for jobname in jobs_needed]
    for a_line in generate_chunks(padded, 7):
        print ' '.join(a_line) + '\n'


def jobs_to_run(geounit, model, units=None, cli_firm_list=None,
                altmaxdist=False):
    """
    Return tuple of job names that need to be run.

    kwargs
    ------
    `units` allows the result of `calc_aermod_units` to be passed to avoid
        multiple loading

    `cli_firm_list` is a list of firms to restrict to, passed down from CLI in
        `batchrun`.
    """
    if units is None:
        units = calc_aermod_units(geounit)

    res = calc_resources(units,
                         cli_facid_list=cli_firm_list,
                         altmaxdist=altmaxdist)
    not_on_disk = check_storage(geounit, model, res, altmaxdist=altmaxdist)
    if hostname == 'harvard':
        # Drop job names that are currently running
        in_queue = get_squeue_names()
        no_disk_no_queue = tuple([jobname for jobname in not_on_disk
                                  if jobname not in in_queue])
    else:
        no_disk_no_queue = not_on_disk

    return no_disk_no_queue


def check_storage(geounit, model, resources, altmaxdist=False):
    """Return tuple of job names for `facid`s whose airq data is not on disk"""

    # Get `facid`s not on disk
    not_on_disk = []
    unique_data = resources[['firm_id', 'num_chunks']].drop_duplicates()
    for facid, firm_id, num_chunks in unique_data.itertuples():
        for chunk_id in xrange(1, num_chunks + 1):
            file_path = normed_firmexp_path(geounit, model, facid,
                                            chunk_info=(chunk_id, num_chunks),
                                            altmaxdist=altmaxdist)
            if not path.isfile(file_path):
                jobname = normed_firmexp_fname(
                    geounit, model, firm_id, chunk_info=(chunk_id, num_chunks)
                )
                not_on_disk.append(jobname)

    return tuple(not_on_disk)


def get_squeue_names():
    """Return array of job names in squeue"""
    p = subprocess.Popen(['squeue', '-u', 'dsulivan'], stdout=subprocess.PIPE)
    p_stdout = p.communicate()[0]
    df = pd.read_table(StringIO.StringIO(p_stdout), sep='\s+')
    in_queue = df['NAME'].unique()

    return in_queue


def cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('geounit',
                        choices=['house', 'monitor', 'block', 'grid'],
                        help='Geographic unit for model')
    parser.add_argument('model', help='Dispersion model')
    parser.add_argument('--altmaxdist', action='store_true')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = cli_args()
    geounit, model, altmaxdist = args.geounit, args.model, args.altmaxdist
    pretty_out(geounit, model, altmaxdist=altmaxdist)
