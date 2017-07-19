"""
Combines Aermod chunks for firms that have too many stacks to run at once.
Currently (2/11/15) meant to be run as __main__.
"""

import os
import shutil
import glob
import argparse

import pandas as pd

from econtools import confirmer

from atmods.io import normed_firmexp_path, parse_firm_info


def main(geounit, model, altmaxdist=False, clean=False, overwrite=False):
    raw_file_patt = normed_firmexp_path(geounit, model, '*', chunk_info=True,
                                        altmaxdist=altmaxdist)
    file_list = glob.glob(raw_file_patt)
    master_list = tuple(file_list)
    print "Handled...",
    import ipdb
    while file_list:
        this_file = file_list.pop()
        firm_id, __, num_chunks = parse_firm_info(this_file)
        dst_filepath = normed_firmexp_path(geounit, model, firm_id,
                                           altmaxdist=altmaxdist)
        ipdb.set_trace()
        if os.path.isfile(dst_filepath) and not overwrite:
            continue
        if num_chunks == 1:
            # Specifically not copy2, want new metadata
            shutil.copy(this_file, dst_filepath)
        else:
            combined_df = combine_chunks(geounit, model, firm_id, num_chunks,
                                         altmaxdist)
            combined_df.to_pickle(dst_filepath)
            del combined_df
            file_list = update_filelist(file_list, firm_id)
        print '\t{}'.format(firm_id)
    print "Done!"
    if clean:
        clean_up(master_list)


def combine_chunks(geounit, model, firm_id, num_chunks, altmaxdist):

    dfs = dict()  # dict to track chunk_id later and track errors
    for chunk_id in range(1, num_chunks + 1):
        this_path = normed_firmexp_path(geounit, model, firm_id,
                                        chunk_info=(chunk_id, num_chunks),
                                        altmaxdist=altmaxdist)
        dfs[chunk_id] = pd.read_pickle(this_path)

    # Do we have all the chunks?
    try:
        assert len(dfs) == num_chunks
    except AssertionError:
        err_str = "Firm {} only had {} out of {} chunks!"
        raise AssertionError(err_str.format(firm_id, len(dfs), num_chunks))

    # Make sure the chunked DataFrames are exactly the same shape, etc.
    first_index, first_columns = dfs[1].index, dfs[1].columns
    for chunk_id in dfs.keys():
        try:
            assert first_index.equals(dfs[chunk_id].index)
            assert first_columns.equals(dfs[chunk_id].columns)
        except AssertionError:
            err_str = "Firm {}'s 1st and {}th chunk have different shapes!"
            raise AssertionError(err_str.format(firm_id, chunk_id))

    # Combine them
    combo_df = dfs.pop(1)
    for chunk_id in dfs.keys():
        combo_df = combo_df.add(dfs[chunk_id])

    return combo_df


def update_filelist(file_list, firm_id):
    """ Removes firm_id's file names from the list."""
    new_file_list = [x for x in file_list if parse_firm_info(x)[0] != firm_id]
    return new_file_list


def clean_up(file_list):
    prompt = "Delete all source files?"
    confirmed = confirmer(prompt)
    if confirmed:
        for fname in file_list:
            os.remove(fname)
        print "All source files deleted!"
    else:
        pass


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('geounit',
                        choices=['house', 'monitor', 'block', 'grid'],
                        help='Geographic unit for model')
    parser.add_argument('model')
    parser.add_argument('--altmaxdist', action='store_true')
    parser.add_argument('--clean', action='store_true')
    parser.add_argument('--overwrite', action='store_true')

    return vars(parser.parse_args())


if __name__ == '__main__':
    kwargs = cli()
    geounit = kwargs.pop('geounit')
    model = kwargs.pop('model')
    main(geounit, model, **kwargs)
