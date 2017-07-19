import re
from os import path
from time import sleep
import pyproj
from geopy import geocoders
import webbrowser

import pandas as pd
import numpy as np

from util import UTM
from util.system import data_path

# This is EPSG:32611
utmz11 = pyproj.Proj(proj='utm', zone=11, ellps='WGS84')

# Google geocodes that are probably wrong
BAD_LOC_TYPES = ('APPROXIMATE', 'GEOMETRIC_CENTER')


def convert_utm(xydf, **kwargs):
    inverse = kwargs.get('inverse', False)
    if inverse:
        inx, iny = _get_utm_vecs(xydf)
    else:
        inx, iny = _get_xy_vecs(xydf)

    outx, outy = utmz11(inx, iny, **kwargs)

    if not inverse:
        outx, outy = np.around(outx).astype(int), np.around(outy).astype(int)

    return outx, outy


def _get_xy_vecs(df, XY=['x', 'y']):

    set_cols = set(df.columns)
    if set(XY) <= set_cols:
        x, y = df[XY[0]].values, df[XY[1]].values
    elif set(UTM) <= set_cols:
        east, north = _get_utm_vecs(df)
        x, y = utmz11(east.values, north.values, inverse=True)
    else:
        ValueError("No xy columns")

    return x, y


def _get_utm_vecs(df):

    if not (set(UTM) <= set(df.columns)):
        tmpdf = df.reset_index()
    else:
        tmpdf = df

    return tmpdf['utm_east'], tmpdf['utm_north']


# Geocoding
def code_series(address_list, addname=None):
    """
    Returns DataFrame with `s.index` and new geocode data.
    Non-matches are omitted.
    """
    # Extract Series
    df = _extract_series(address_list, addname)
    all_results = []
    for idx, address in df.iteritems():
        print idx
        result = code_address(address)
        if result is not None:
            result.name = idx
            all_results.append(result)

    return_df = pd.DataFrame(all_results)
    return_df.index.names = df.index.names

    # Also return input 'address'
    return_df = return_df.join(df)

    return return_df

def _extract_series(df, addname):  # noqa
    if df.ndim == 1 or df.shape[1] == 1:
        return df.squeeze()
    elif not addname:
        raise ValueError
    else:
        return df[addname].squeeze()


def code_address(address):
    result = call_coder(address)
    clean_result = _extract_geo_results(result)
    return clean_result

def _extract_geo_results(georesult):  # noqa

    if georesult is None:
        return None

    d = dict()
    d['match_address'] = georesult.address
    d['lat'] = georesult.latitude
    d['lon'] = georesult.longitude
    d['match_type'] = georesult.raw['types'][0]
    d['location_type'] = georesult.raw['geometry']['location_type']
    return pd.Series(d)


def call_coder(address, nozip=False):
    # XXX put this key else where...
    api_key = 'AIzaSyAScRWTK6tB2rNPmFZUidym-IlfULn2mM0'
    coder = geocoders.GoogleV3(api_key=api_key)
    sleep(0.3)

    if nozip:
        zip_re = '\s\d{5}(-\d{0,4}){0,1}$'
        address = re.sub(zip_re, '', address)

    result = coder.geocode(address, exactly_one=True)

    if _try_again(result, nozip):
        return call_coder(address, nozip=True)
    else:
        return result

def _try_again(result, nozip):  # noqa

    if nozip:
        return False
    elif result is None:
        return True
    elif result.raw['geometry']['location_type'] in BAD_LOC_TYPES:
        return True
    else:
        return False


# Address cleaning
def cleanadds(indf, outname='clean_address',
              street='street', city='city', state='state', zipname='zip',
              nozip=False):
    """Returns Series with original index and column `outname`."""

    df = indf[[street, city, state, zipname]].copy()

    df['clean_street'] = df[street]
    df['clean_city'] = df[city]
    df['clean_state'] = df[state]
    df['clean_zip'] = df[zipname]

    df['clean_zip'] = df['clean_zip'].apply(clean_zip)

    df['clean_street'] = df['clean_street'].apply(clean_street)

    df[outname] = df.apply(_join_address, axis=1)

    df[outname] = df[outname].str.upper()

    if nozip:
        df[outname] = df[outname].replace('\s\d{5}(-\d{0,4}){0,1}$', '',
                                          regex=True)

    return df[outname]

def _join_address(x):  # noqa
    skelstr = '{}, {}, {}{}'
    zipcode = ' ' + x['clean_zip'] if x['clean_zip'] else ''
    add_str = skelstr.format(
        x['clean_street'], x['clean_city'], x['clean_state'], zipcode)
    return add_str


def clean_street(street):
    street = street.strip().upper()
    # Remove commas, apostrophes
    re_matches = ["\.", "'"]
    for regex in re_matches:
        street = re.sub(regex, '', street)
    # Remove extra spaces
    street = re.sub('\s+', ' ', street)
    # Standardize commonly abbreviated words
    st_abbrev = _abbrev_dict()
    for regex, std_word in st_abbrev.iteritems():
        street = re.sub(regex, std_word, street)

    return street

def _abbrev_dict():
    st_abbrev = {'STREET':      'ST',
                 'ROAD':        'RD',
                 'AVENUE':      'AV',
                 'AVE':         'AV',
                 'HIGHWAY':     'HWY',
                 'DRIVE':       'DR',
                 'PLACE':       'PL',
                 'BLVD':        'BL',
                 'BOULEVARD':   'BL',
                 'COURT':       'CT',
                 'NORTH':       'N',
                 'SOUTH':       'S',
                 'EAST':        'E',
                 'WEST':        'W '}
    match_beg = r'(\s+)'    # Leading spaces
    match_end = r'(\s+|$)'  # Trailing spaces or EOL
    keep_beg = r'\g<1>'     # Keep a leading space if one's there
    keep_end = r'\g<2>'     # Keep a trailing space if one's there
    tempdict = dict()
    for key, item in st_abbrev.iteritems():
        tempdict[match_beg + key + match_end] = keep_beg + item + keep_end
    st_abbrev = tempdict

    return st_abbrev


def clean_zip(zipcode):
    # Force string,
    if isinstance(zipcode, str):
        pass
    elif np.isnan(zipcode):
        zipcode = ''
    else:
        zipcode = str(int(zipcode))
    # No ending dash
    zipcode = re.sub('-\s*$', '', zipcode)
    # leading zeros
    if '-' in zipcode:
        mainzip, zip4 = zipcode.split('-')
        mainzip = mainzip.zfill(5)
        zipcode = '-'.join([mainzip, zip4])
    elif len(zipcode) > 0:
        zipcode = zipcode.zfill(5)

    return zipcode


# Show stuff on maps
def draw_googlemap(x, y, filepath=None, **mapargs):
    # Handle filepath
    import pygmaps
    map_path = data_path('tmp', 'maps')
    if filepath is None:
        import tempfile
        _, filepath = tempfile.mkstemp(dir=map_path,
                                       prefix='tmpmap',
                                       suffix='.html')
    else:
        filepath = path.join(map_path, filepath) + '.html'

    zoomlevel = 16
    x0, y0 = np.mean(x), np.mean(y)
    this_map = pygmaps.maps(y0, x0, zoomlevel)
    assert len(x) == len(y)
    # Draw it!
    color = mapargs.pop('color', '#0000FF')
    titles = mapargs.pop('titles', None)
    if titles:
        iterator = titles
    else:
        iterator = range(len(x))
    for i in iterator:
        this_map.addpoint(y[i], x[i], color=color)  # , title=i)  Newer version?
    this_map.draw(filepath)
    webbrowser.open_new_tab(filepath)
