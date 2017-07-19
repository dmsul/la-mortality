"""

"""

import numpy as np
import pandas as pd
import subprocess
import os
import re
import sys
import time
import resource


print "Quoth the Raven"


def aermapChunk(inFrame, is_receptor, UTMZONE, AERMAP_PATH, CALL_PATH, ID):
    """
        Almost works. Something is wrong with the merging at the end.
    """

    # Change working directory (All files will be written here.)
    os.chdir(CALL_PATH)

    # Initialize output frame
    outFrame = inFrame.ix[
        :, ['utm_east', 'utm_north']].reset_index(
        level=0).set_index(
        ['utm_east', 'utm_north'])
    outFrame['elev'] = 0
    if is_receptor:
        outFrame['hill'] = 0

    # Control Pathway for INP

        # Begin INP file string
    INP_STRING = (
        "CO STARTING\n"
        "   TITLEONE 'Just an Old Fashioned AERMAP'\n"
        "   DATATYPE NED\n")

        # Determine geographic area

            # Region boundaries
    MIN_EAST, MIN_NORTH, MINX, MINY = inFrame[
        ['utm_east', 'utm_north', 'x', 'y']].min(
        axis=0)
    MAX_EAST, MAX_NORTH, MAXX, MAXY = inFrame[
        ['utm_east', 'utm_north', 'x', 'y']].max(
        axis=0)
    MEAN_EAST, MEAN_NORTH = inFrame[['utm_east', 'utm_north']].mean(axis=0)

            # Translate to upper-left corners
    filelist_w = np.arange(np.ceil(-MAXX), np.ceil(-MINX)+1).astype(np.int8)
    filelist_n = np.arange(np.ceil(MINY), np.ceil(MAXY)+1).astype(np.int8)

            # Add files to INP
    for x in filelist_w:
        for y in filelist_n:
            INP_STRING += "   DATAFILE ned_tifs/imgn%sw%s_1.tif\n" % (
                str(y), str(x))

            # Add domain string (can use lat/long or UTM)
    buffer = 5000
    domain_tup = (
        MIN_EAST -
        buffer,
        MIN_NORTH -
        buffer,
        UTMZONE,
        MAX_EAST +
        buffer,
        MAX_NORTH +
        buffer,
        UTMZONE)
    INP_STRING += "   DOMAINXY %d %d %d %d %d %d\n" % domain_tup

            # Add (pointless) anchor point
    INP_STRING += "   ANCHORXY %d %d %d %d %d %d\n" % (MEAN_EAST,
                                                       MEAN_NORTH,
                                                       MEAN_EAST,
                                                       MEAN_NORTH,
                                                       UTMZONE,
                                                       0)

        # Finish control pathway
    INP_STRING += "   RUNORNOT RUN\nCO FINISHED\n\n"

    # SourcePathway for INP
    if not is_receptor:
        INP_STRING += "SO STARTING\n"
        tempid = 1
        for source in inFrame.index.values:
            INP_STRING += "   LOCATION Src%d POINT %d %d\n" % (tempid,
                                                               inFrame.ix[
                                                                   source,
                                                                   'utm_east'],
                                                               inFrame.ix[
                                                                   source,
                                                                   'utm_north'])
            tempid += 1
        INP_STRING += "SO FINISHED\n\n"

        # Create fake receptor DataFrame (must have at least one receptor)
        receptorFrame = pd.DataFrame(
            np.array([MEAN_EAST, MEAN_NORTH]), columns=['utm_east', 'utm_north'])
    else:
        receptorFrame = inFrame

    # Receptor Pathway for INP
    INP_STRING += "RE STARTING\n"
    for receptor in receptorFrame.index.values:
        INP_STRING += "   DISCCART %d %d\n" % (receptorFrame.ix
                                               [receptor, 'utm_east'],
                                               receptorFrame.ix
                                               [receptor, 'utm_north'])
    INP_STRING += "RE FINISHED\n\n"

    # Output Pathway for INP
    INP_STRING += ("OU STARTING\n"
                   "   RECEPTOR  receptor_output.txt\n")
    if not is_receptor:
        INP_STRING += "   SOURCLOC  source_output.txt\n"
    INP_STRING += "OU FINISHED\n"
    with open('aermap.inp', 'w') as f:
        f.write(INP_STRING)
    print "File Written!"

    # Call Aermap
    subprocess.call('./aermap_unix.out')

    # Clean output
    if is_receptor:
        RAW_DATA = 'receptor_output.txt'
        begin_crap = '^\s*DISCCART\s*'
        col_names = ['utm_east', 'utm_north', 'elev', 'hill']
    else:
        RAW_DATA = 'source_output.txt'
        begin_crap = '^SO.*POINT\s*'
        col_names = ['utm_east', 'utm_north', 'elev']

    with open(RAW_DATA, 'r') as f:
        tempfile = re.sub('^.*METERS\n', '', f.read(), flags=re.DOTALL)
        tempfile = re.sub(begin_crap, '', tempfile, flags=re.MULTILINE)

    with open('new_out.txt', 'w') as f:
        # f.seek(0)
        f.write(tempfile)
        del tempfile
    raw_results = pd.read_table(
        'new_out.txt',
        delim_whitespace=True,
        names=col_names,
        index_col=[
            'utm_east',
            'utm_north'])

    print raw_results

    # Merge back to DF with ID's as index
    outFrame = outFrame.add(
        raw_results,
        fill_value=0).reset_index().set_index(ID)

    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"
    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"
    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"
    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"
    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"
    print "HAS THE OUTPUT MATRIX BEEN FIXED YET?!?!?!"

    return outFrame


print "Nevermore."
