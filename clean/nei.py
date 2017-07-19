from os import path

import pandas as pd
import numpy as np

from econtools import load_or_build
from util.system import src_path


def load_nei(year, **kwargs):
    CA = kwargs.get('CA', True)

    if year == 1999:
        return load_1999_fullemit(**kwargs)
    elif year == 2002:
        base_path = nei_path('2002', 'nei2002.p')
    elif year == 2005:
        base_path = nei_path('2005', 'nei2005.p')
    else:
        raise ValueError

    filepath = get_picklename(base_path, CA)

    @load_or_build(filepath)
    def _load_nei_mid(year, **kwargs):
        df = _load_nei_guts(year, **kwargs)
        return df

    return _load_nei_mid(year, **kwargs)


# NEI 2002, 2005
def _load_nei_guts(year, CA=True, rebuild=False, _load=True):

    if year == 2002:
        raw_path = nei_path('2002', 'ALLNEI_CAP_11302007.csv')
    elif year == 2005:
        raw_path = nei_path('2005', 'nei_2005_mastertable.csv')

    df = pd.read_csv(raw_path)

    # Rename and restrict columns
    col_names = uniform_names(year)
    keep_cols = [x for x in df.columns if x in col_names]
    df = df[keep_cols]
    df = df.rename(columns=col_names)

    # Restrict to California
    if CA:
        df = df[df['state'] == 'CA']

    # Fix Unicode problem
    types = df.dtypes
    for col in types[types == 'object'].index:
        df[col] = df[col].astype(str)

    # Fix fips codes
    df['fips'] = df['fips'].astype(int).astype(str).str.zfill(5)
    df['fips_state'] = df['fips'].str[:2].astype(str)
    df['fips_county'] = df['fips'].str[2:].astype(str)
    del df['fips']

    # Make SIC consistent
    if df['sic'].dtype == 'O':
        df['sic'] = df['sic'].apply(lambda x: x if x.isdigit() else np.nan)
        df['sic'] = df['sic'].astype(float)

    # Misc crap
    if year == 2002:
        df.loc[(df['state_id'] == '301026115389') &
               (df['stack_default_flag'] == 99999), 'stack_default_flag'] = 0
        df.loc[(df['state_id'] == '191026115536') &
               (df['stack_default_flag'] == 99999), 'stack_default_flag'] = 0

    # IDK?
    site_vars = ['state_id', 'nei_id', 'name', 'sic',
                 'address', 'city', 'state', 'zip', 'county']
    stack_vars = ['state_id', 'release_point_id', 'release_point_type',
                  'release_point_desc',
                  'stack_ht_ft', 'stack_diam_f',
                  'stack_temp_f', 'stack_veloc_ftsec',
                  'stack_default_flag',
                  'lon', 'lat', 'xy_default_flag']
    emit_vars = ['state_id', 'release_point_id', 'process_id', 'scc',
                 'emissions', 'emi_text', 'control_status', 'data_source']

    return df

def uniform_names(year):  #noqa
    if year == 2002:
        uniform_names = {
            'County':               'county',
            'StateCountyFIPS':      'fips',
            'StateFacilityID':      'state_id',
            'NEISiteID':            'nei_id',
            'FacilityName':         'name',
            'SIC Code':             'sic',
            'NAICE Code':           'naics',
            'EmisssionUnitID':       'emission_unit_id',
            'ProcessID':            'process_id',
            'SCC':                  'scc',
            'EmissionReleasePointID':   'release_point_id',
            'EmissionReleasePointTypeCode':   'release_point_type',
            'EmissionReleasePointTypeDescription':   'release_point_type_desc',
            'Pollutant Code':       'pollutant_code',
            # 'Emissions (tpy)':      'emissions_lowprecision',
            'Emissions (tpy text)': 'emissions',
            'EMISSIONS_CALC_METHOD_CODE':   'emissions_calc_method_code',
            'Control Status':       'control_status',
            'LocationAddress':      'street',
            'City':                 'city',
            'State':                'state',
            'ZipCode':              'zip',
            'StackHeight (ft)':     'stack_ht_ft',
            'ExitGasTemperature (F)':   'stack_temp_f',
            'StackDiameter (ft)':   'stack_diam_ft',
            'ExitGasVelocity (ft/sec)': 'stack_veloc_ftsec',
            'StackDefaultFlag':     'stack_default_flag',
            'Data Source code':     'data_source_code',
            'Data Source':          'data_source',
            'dblXCoordinate':       'lon',
            'dblYCoordinate':       'lat',
            'LocationDefaultFlag':  'xy_default_flag'
        }
    elif year == 2005:
        uniform_names = {
            'STATE_AND_COUNTY_FIPS_CODE': 'fips',
            'STATE_ABBREV':         'state',
            'STATE_FACILITY_IDENTIFIER': 'state_id',
            'NTI_SITE_ID':          'nei_id',
            'FACILITY_NAME':        'name',
            'LOCATION_ADDRESS':     'street',
            'CITY':                 'city',
            'ZIPCODE':              'zip',
            'SIC_PRIMARY':          'sic',
            'NAICS_PRIMARY':        'naics',
            'EMISSION_UNIT_ID':     'emission_unit_id',
            'PROCESS_ID':           'process_id',
            'SCC':                  'scc',
            'EMISSION_RELEASE_POINT_ID': 'release_point_id',
            'EMISSION_RELEASE_POINT_TYPE': 'release_point_type',
            'STACK_HEIGHT':         'stack_ht_ft',
            'EXIT_GAS_TEMPERATURE': 'stack_temp_f',
            'STACK_DIAMETER':       'stack_diam_ft',
            'EXIT_GAS_VELOCITY':    'stack_veloc_ftsec',
            'STACK_DEFAULT_FLAG':   'stack_default_flag',
            'X_COORDINATE':         'lon',
            'Y_COORDINATE':         'lat',
            'LOCATION_DEFAULT_FLAG': 'location_default_flag',
            'POLLUTANT_CODE':       'pollutant_code',
            'ANNUAL_EMISSIONS':     'emissions',
            'CONTROL_STATUS':       'control_status'
        }

    return uniform_names


# NEI 1999
def load_1999_fullemit(CA=True, _rebuild_down=False):
    """Load 1999 data as one table (like 2002 and 2005)"""

    unique_stack_id = ['fips_state', 'fips_county', 'state_id',
                       'release_point_id']
    unique_id = unique_stack_id + ['emission_unit_id', 'process_id']

    si = load_nei1999_table('si', CA=CA, _rebuild=_rebuild_down)
    em = load_nei1999_table('em', CA=CA, _rebuild=_rebuild_down)
    ep = load_nei1999_table('ep', CA=CA,
                            _rebuild=_rebuild_down)[unique_id + ['scc']]
    keep_stack_vars = (unique_stack_id +
                       ['stack_ht_ft', 'stack_diam_ft', 'stack_temp_f',
                        'stack_veloc_ftsec', 'lat', 'lon'])
    er = load_nei1999_table('er', CA=CA,
                            _rebuild=_rebuild_down)[keep_stack_vars]

    # Fix duplicate time coverage
    for date in ['start_date', 'end_date']:
        em[date] = pd.to_datetime(em[date].astype(str), format='%Y%m%d')
    em['duration'] = (em['end_date']-em['start_date']).astype('timedelta64[D]')
    em = em.sort_values(unique_id + ['pollutant_code', 'duration'])
    em = em.drop_duplicates(unique_id, keep='last')

    # Restrict emission variables
    em = em[unique_id + ['pollutant_code', 'emissions']]

    full = pd.merge(em, ep, on=unique_id, how='left')
    full = pd.merge(full, er, on=unique_stack_id, how='left')
    full = pd.merge(si, full, on=unique_stack_id[:-1], how='right')

    # Make sure id's are actually unique
    tmp = full.set_index(unique_id + ['pollutant_code'])
    assert tmp.index.is_unique

    # Make SIC consistent
    if full['sic'].dtype == 'O':
        full['sic'] = full['sic'].apply(lambda x: x if x.isdigit() else np.nan)
        full['sic'] = full['sic'].astype(float)

    # Make fips_state consistent
    full['fips_state'] = full['fips_state'].astype(str)

    return full


def load_nei1999_table(table, **kwargs):
    CA = kwargs.get('CA', True)
    base_path = nei1999_table_src_path(table, CA)
    filepath = get_picklename(base_path)

    @load_or_build(filepath)
    def _load_mid(table, **kwargs):
        return _load_nei1999_table_guts(table, **kwargs)

    return _load_mid(table, **kwargs)


def _load_nei1999_table_guts(table, CA=True):
    """
    Tables:
    --------
    'si': site (facility meta-data)
    'ep': emission process (scc; process, unit, and release point id's)
    'er': emission release point (stack parameters, release point id)
    'em': emissions (emi; process, release id's; pollutant type (NOx))
    """

    # Read raw table
    colnames, colnumbers = _nei1999_fixedwidth_params(table)
    filepath = nei1999_table_src_path(table)
    df = pd.read_fwf(filepath, header=None, colspecs=colnumbers, names=colnames)

    df = _table_cleaning(df, table, CA=CA)

    return df

def _nei1999_fixedwidth_params(table):  #noqa

    column_maps = {
        'si': (
            ('record_type', (0, 2)),
            ('fips_state', (2, 4)),
            ('fips_county', (4, 7)),
            ('state_id', (7, 22)),
            ('facility_registry_id', (22, 34)),
            ('facility_category', (34, 36)),
            ('oris_code', (36, 42)),
            ('sic', (42, 46)),
            ('naics', (46, 52)),
            ('name', (52, 132)),
            ('site_description', (132, 172)),
            ('street', (172, 222)),
            ('city', (222, 282)),
            ('state', (282, 284)),
            ('zip', (284, 298)),
            ('country', (298, 338)),
            ('nti_site_id', (338, 358)),
            ('dun_brad', (358, 367)),
            ('tri_id', (367, 387)),
            ('submittal_flag', (387, 391)),
            ('tribal_code', (391, 394))
        ),
        'er': (
            ('record_type', (0, 2)),
            ('fips_state', (2, 4)),
            ('fips_county', (4, 7)),
            ('state_id', (7, 22)),
            ('release_point_id', (28, 34)),
            ('release_point_type', (34, 36)),
            ('stack_ht_ft', (46, 56)),
            ('stack_diam_ft', (56, 66)),
            ('stackfencedistft', (66, 74)),
            ('stack_temp_f', (74, 84)),
            ('stack_veloc_ftsec', (84, 94)),
            ('stackexitflowf3s', (94, 104)),
            ('lon', (104, 115)),
            ('lat', (115, 125)),
            ('utmzone', (125, 127)),
            ('xy_type', (127, 135)),                # LATLON or UTM
            ('horizontal_area_fug', (135, 143)),
            ('release_ht_fug', (143, 151)),
            ('fug_dim_unit', (151, 161)),
            ('emitreleaseptdesc', (161, 241)),
            ('submit_flag', (241, 245)),
            ('horiz_collect_method', (245, 248)),
            ('horiz_accuracy_m', (248, 254)),
            ('horiz_ref_datum', (254, 257)),
            ('ref_point_code', (257, 260)),
            ('sourcemapscalenum', (260, 270)),
            ('xy_data_source', (270, 273)),
            ('tribal_code', (273, 275))
        ),
        'ep': (
            ('record_type', (0, 2)),
            ('fips_state', (2, 4)),
            ('fips_county', (4, 7)),
            ('state_id', (7, 22)),
            ('emission_unit_id', (22, 28)),
            ('release_point_id', (28, 34)),
            ('process_id', (34, 40)),
            ('scc', (40, 50)),
            ('processMACTCode', (50, 56)),
            ('emissionProcessDescripton', (56, 134)),
            ('winterThroughputPCT', (134, 137)),
            ('springThroughputPCT', (137, 140)),
            ('summerThroughputPCT', (140, 143)),
            ('fallThroughputPCT', (143, 146)),
            ('annualAvgDaysPerWeek', (146, 147)),
            ('annualAvgWeeksPerYear', (147, 149)),
            ('annualAvgHoursPerDay', (149, 151)),
            ('annualAvgHoursPerYear', (151, 155)),
            ('heatContent', (155, 163)),
            ('sulfurContent', (163, 168)),
            ('ashContent', (168, 173)),
            ('processMACTComplianceStatus', (173, 179)),
            ('submittalFlag', (179, 183)),
            ('tribalCode', (183, 186))
        ),
        'em': (
            ('record_type',             (0, 2)),
            ('fips_state',              (2, 4)),
            ('fips_county',             (4, 7)),
            ('state_id',                (7, 22)),
            ('emission_unit_id',        (22, 28)),
            ('process_id',              (28, 34)),
            ('pollutant_code',          (34, 43)),
            ('release_point_id',        (50, 56)),
            ('start_date',              (56, 64)),
            ('end_date',                (64, 72)),
            ('start_time',              (72, 76)),
            ('end_time',                (76, 80)),
            ('emissions',               (90, 100)),  # Cols correct to here
            # ('empty, should be emitUnitNumerator', (100, 110)),
            ('emissionUnitNumerator',   (100 + 10, 110 + 10)),
            # ('emissionType', (110, 112)),
            # ('emReliabilityIndicator', (112, 117)),
            ('factorNumericValue',      (117 + 10, 127 + 10)),
            ('factorUnitNumerator',     (127 + 10, 137 + 10)),
            ('factorUnitDenominator',   (137 + 10, 147 + 10)),
            ('material',                (147 + 10, 151 + 10)),
            # ('materialIO', (151, 161)),
            ('emissions_calc_method_code', (166 + 10, 168 + 10)),
            # ('efReliabilityIndicator', (168, 173)),
            # ('ruleEffectiveness', (173, 178)),
            # ('ruleEffectivenessMethod', (178, 180)),
            # ('hapEmissionsPerformanceLevel', (183, 185)),
            ('control_status',          (185 + 10, 197 + 10)),
            ('emissions_data_level',    (197 + 10, 207 + 10)),
            ('submittalFlag',           (207 + 10, 211 + 10)),
            ('tribalCode',              (211 + 10, 214 + 10))
        )
    }

    names = [x[0] for x in column_maps[table]]
    colnumbers = [x[1] for x in column_maps[table]]

    return names, colnumbers

def _table_cleaning(df, table, CA=True): #noqa

    df = df[df['record_type'] == table.upper()]

    if table == 'si':       # Site
        df = df[df['record_type'].notnull()]
        df['zip'] = df['zip'].str.replace('UNKNOWN', '')
        # XXX think about this (dropping zip4)
        df['zip'] = df['zip'].replace('^(\d+)[-]{0,1}.*$', '\g<1>', regex=True)

    if CA:
        if table in ('er', 'ep', 'em'):
            assert df['fips_state'].dtype == 'object'
            df = df[df['fips_state'] == '6']
        elif table == 'si':
            df = df[df['state'] == 'CA']

    # Fix Unicode problem
    types = df.dtypes
    for col in types[types == 'object'].index:
        df[col] = df[col].astype(str)

    # Make sure `fips_county` is str3
    df['fips_county'] = df['fips_county'].astype(int).astype(str).str.zfill(3)

    return df


def nei1999_table_src_path(table):
    return nei_path('1999', '{}point.txt').format(table)


# Filename aux
def nei_path(*args):
    return src_path('nei', *args)


def get_picklename(filepath, CA):
    fileroot, fileext = path.splitext(filepath)
    if CA:
        ca_suffix = '_CA'
    else:
        ca_suffix = ''
    pickle_path = fileroot + ca_suffix + '.p'
    return pickle_path


if __name__ == '__main__':
    df = load_nei(2005)
