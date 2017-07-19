from clean.house.main import load_house_sample, load_houses_places, load_houses_utm
from clean.house.blockid import load_houses_block2000
from clean.monitor import load_monitors
from clean.block import load_blocks_wzip, load_blockzips, load_blocks
from clean.bg import load_bgdata, load_bg_area
from clean.fake_grid import load_master_grid, GRID_SIZE
# flake8: noqa


def load_geounit(geounit):

    if geounit == 'block':
        df = load_blocks()
    elif geounit == 'house':
        df = load_house_sample()
    elif geounit == 'monitor':
        df = load_monitors()
    elif geounit == 'grid':
        df = load_master_grid()
    else:
        raise ValueError("Geounit `{}` invalid".format(geounit))

    return df


def std_cities(df):
    df.replace({'city': {
        'SURFSIDE': 'SEAL BEACH',   # Gated community, Really part of Seal Beach
        'PLS VRDS PNSL': 'PALOS VERDES PENINSULA',
        'PALOS VERDES PENINSULA C': 'PALOS VERDES PENINSULA',
        'MANHATTAN BCH': 'MANHATTAN BEACH',
        'HUNTINGTN BCH': 'HUNTINGTON BEACH',
        'HUNTINGTON PK': 'HUNTINGTON PARK',
        'RCH PALOS VRD': 'RANCHO PALOS VERDES',
        'RLLNG HLS EST': 'ROLLING HILLS ESTATES',
        'HAWAIIAN GDNS': 'HAWAIIAN GARDENS',
        'E RNCHO DMNGZ': 'COMPTON',     # Compton neighborhoods
        'RANCHO DOMINGUEZ': 'COMPTON',
        'ROSEWOOD': 'COMPTON',
    }},
        inplace=True)
