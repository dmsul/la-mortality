from os import path
import pandas as pd
import numpy as np

from econtools import stata_merge, load_or_build, force_iterable

from util.system import data_path, src_path
from util.addressmeister import MatchAdd
from clean.fhm.rawio import load_firmpanel

# XXX: Still missing about 30 non-electrics, but all the big ones are covered
# TODO: Cross-validate imputations
# TODO: Standardize names across years
# TODO: In general this is a mess

NEI_PATH = src_path('nei')

STACK_VARS_2002 = ['stackheightft', 'stackdiameterft', 'exitgastemperaturef',
                   'exitgasvelocityftsec']


def load_stacks(firm_id=None):
    # TODO: deprecate?
    """Read full firm/stack file, grab the firm_id(s) requested"""
    stacks = build_stacks()

    # XXX Firm_id should be int. Make sure it is
    if not np.issubdtype(stacks['firm_id'].dtype, np.integer):
        stacks['firm_id'] = stacks['firm_id'].astype(int)

    if firm_id is None:
        return_stacks = stacks
    else:
        firm_id = set(force_iterable(firm_id))
        # Check that firm_id is valid (they don't all have stacks)
        try:
            good_ids = set(stacks['firm_id'].unique())
            assert firm_id <= good_ids
        except AssertionError:
            bad_ids = list(firm_id.difference(good_ids))
            err_str = "Firm_ids '{}' is not a valid firm id!".format(bad_ids)
            raise ValueError(err_str.format(firm_id))

        return_stacks = stacks[stacks['firm_id'].isin(firm_id)]

    return return_stacks


@load_or_build(data_path('firms_stacks.dta'))
def build_stacks():

    PanelMaker = StackPanel(new_addmatch=True)

    firms99 = load_firm_data(1999)
    firms_stacks1999 = PanelMaker.getstacks(firms99, 1999)

    firms02 = load_firm_data(2002)
    firms_stacks2002 = PanelMaker.getstacks(firms02, 2002)

    raw_shares = calc_fugitive_share(firms_stacks2002)
    cleaned_2002 = clean_ufacids_stacks(firms_stacks2002)

    # Special cleaning for firm(s) in 99 but not 02
    all_firms = set(firms02.index.unique())
    matched_firms = set(cleaned_2002['ufacid'].unique())
    unmatched_firms = all_firms.difference(matched_firms)

    only99 = firms_stacks1999[firms_stacks1999['ufacid'].isin(unmatched_firms)]
    clean_only99 = clean_ufacids_stacks(only99, nei_year=2005, impute_band=.5)

    clean_all = cleaned_2002.append(clean_only99)

    # Pare down, collapse, etc.
    firm_stack_group = clean_all.groupby(['ufacid'] + STACK_VARS_2002)
    combined_stacks = pd.DataFrame(firm_stack_group['emissions'].sum())

    stack_totals = combined_stacks.groupby(level='ufacid')['emissions'].sum()

    comb_stack_shares = combined_stacks.apply(
        lambda x: x['emissions'] / stack_totals[x.name[0]], axis=1)

    comb_stack_shares = pd.DataFrame(comb_stack_shares, columns=['emit_share'])

    # Bring in firm-specific data (metsite, UTM)
    firm_static_data = load_firm_data(static=True)

    aermod_ready = stata_merge(
        comb_stack_shares.reset_index(), firm_static_data,
        assertval=3, on='ufacid', how='left')

    # Convert from English to metric
    ft_to_m = 0.3048
    for col in ['stackheightft', 'stackdiameterft', 'exitgasvelocityftsec']:
        aermod_ready[col] = aermod_ready[col] * 0.3048
    aermod_ready['exitgastemperaturef'] = (
        aermod_ready['exitgastemperaturef'] + 459.67) * 5. / 9

    # Rename
    newstacks = {
        'stackheightft': 'stack_ht',
        'stackdiameterft': 'stack_diam',
        'exitgastemperaturef': 'stack_temp',
        'exitgasvelocityftsec': 'stack_veloc'
    }
    aermod_ready.rename(columns=newstacks, inplace=True)

    return aermod_ready


def _make_nei_id(x):
    ABCODE_XWALK = {'GBV': 1, 'LC':  2, 'LT':  3, 'MC':  4, 'MD':  5, 'NC':  6,
                    'NCC': 7, 'NEP': 8, 'OCS': 9, 'SC':  10, 'SCC': 11, 'SD':
                    12, 'SF':  13, 'SJV': 14, 'SS':  15, 'SV':  16, }
    DISCODE_XWALK = {
        '': '', 'AMA': '', 'AV':  2, 'BA':  3, 'BUT': '', 'COL': '', 'ED':  23,
        'FR':  8, 'GBU': 9, 'GLE': 10, 'IMP': 11, 'KER': 12, 'LAK': 13, 'LAS':
        14, 'MBU': 15, 'MEN': 16, 'MOD': 17, 'MOJ': 18, 'MPA': 19, 'NCU': 20,
        'NS': 21, 'NSI': 22, 'PLA': 23, 'SAC': 24, 'SB':  25, 'SC':  26, 'SD':
        27, 'SHA': 28, 'SIS': 29, 'SJU': 30, 'SLO': 31, 'TEH': 8, 'TUO': 33,
        'VEN': 34, 'YS': 35,
    }

    try:
        co = str(int(x['co']))
    except ValueError:
        co = ''
    abcode = str(ABCODE_XWALK[x['ab']]).rjust(2, '0')
    discode = str(DISCODE_XWALK[x['dis']]).rjust(2, '0')
    facid = str(int(x['facid']))
    nei_id = co + abcode + discode + facid
    return nei_id


def load_firm_data(year=0, static=False):
    firms = load_firmpanel()

    if static:
        keep_vars = ['ufacid', 'electric', 'firm_id', 'Tfirm_id', 'utm_east',
                     'utm_north', 'pop1990', 'metsite_code', 'metsite_year',
                     'metsite_z']
        firms = firms[keep_vars].drop_duplicates()

    else:
        firms['my_stateID'] = firms.apply(_make_nei_id, axis=1)
        keep_vars = ['ufacid', 'fname', 'fstreet', 'fcity', 'fzip', 'fsic',
                     'my_stateID', 'electric']
        # XXX:  This one firm is electric, but not flagged electric, and has
        #       missing meta-info in 1999, 2002. Throw him in with 1999 (he
        #       ceases to exist around 2001?).
        if year == 1999:
            weirdo = firms.loc[(firms['ufacid'] == '19_SC_SC_112853') &
                               (firms['year'] == 2001), keep_vars]
        else:
            weirdo = None

        firms = firms.loc[firms['year'] == year, keep_vars]

        if weirdo is not None:
            firms = firms[~(firms['ufacid'] == '19_SC_SC_112853')]
            firms = firms.append(weirdo)

        # XXX for now just focus on one electric status at a time
        # firms = firms[firms['electric'] == 0]
        firms.set_index('ufacid', inplace=True)

    return firms


class read_rawNEI1999(object):

    def __init__(self):
        self.filepath = path.join(NEI_PATH, '1999', '{}point.txt')

        self.column_maps = {
            'si': (
                ('recordType', (0, 2)),
                ('fips_state', (2, 4)),
                ('fips_county', (4, 7)),
                ('stateID', (7, 22)),
                ('facilityRegistryID', (22, 34)),
                ('facilityCategory', (34, 36)),
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
                ('nti_siteID', (338, 358)),
                ('dun_brad', (358, 367)),
                ('tri_id', (367, 387)),
                ('submittal_flag', (387, 391)),
                ('tribal_code', (391, 394))
            ),
            'er': (
                ('recordType', (0, 2)),
                ('fips_state', (2, 4)),
                ('fips_county', (4, 7)),
                ('stateID', (7, 22)),
                ('emissionReleasePointID', (28, 34)),
                ('emissionReleasePointType', (34, 36)),
                ('stackheightft', (46, 56)),
                ('stackdiamft', (56, 66)),
                ('stackfencedistft', (66, 74)),
                ('stackexittempf', (74, 84)),
                ('stackexitvelocityfs', (84, 94)),
                ('stackexitflowf3s', (94, 104)),
                ('xcoord', (104, 115)),
                ('ycoord', (115, 125)),
                ('utmzone', (125, 127)),
                ('xy_type', (127, 135)),
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
                ('coorddatasourcecode', (270, 273)),
                ('tribal_code', (273, 275))
            ),
            'ep': (
                ('recordType', (0, 2)),
                ('fips_state', (2, 4)),
                ('fips_county', (4, 7)),
                ('stateID', (7, 22)),
                # ('recordType', (0, 2)),
                # ('stateCountyFips', (2, 7)),
                # ('stateFacilityIdentifier', (7, 22)),
                ('emissionUnitID', (22, 28)),
                ('emissionReleasePointID', (28, 34)),
                ('processID', (34, 40)),
                ('SCC', (40, 50)),
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
                ('recordType', (0, 2)),
                ('fips_state', (2, 4)),
                ('fips_county', (4, 7)),
                ('stateID', (7, 22)),
                ('emissionUnitID', (22, 28)),
                ('processID', (28, 34)),
                ('pollutantCode', (34, 43)),
                ('emissionReleasePointID', (50, 56)),
                ('startDate', (56, 64)),
                ('endDate', (64, 72)),
                ('startTime', (72, 76)),
                ('endTime', (76, 80)),
                ('emissionNumericValue', (90, 100)),  # Cols correct to here
                # ('empty, should be emitUnitNumerator', (100, 110)),
                ('emissionUnitNumerator', (100 + 10, 110 + 10)),  #
                # ('emissionType', (110, 112)),
                # ('emReliabilityIndicator', (112, 117)),
                ('factorNumericValue', (117 + 10, 127 + 10)),
                ('factorUnitNumerator', (127 + 10, 137 + 10)),
                ('factorUnitDenominator', (137 + 10, 147 + 10)),
                ('material', (147 + 10, 151 + 10)),
                # ('materialIO', (151, 161)),
                ('emissionCalculationMethodCode', (166 + 10, 168 + 10)),
                # ('efReliabilityIndicator', (168, 173)),
                # ('ruleEffectiveness', (173, 178)),
                # ('ruleEffectivenessMethod', (178, 180)),
                # ('hapEmissionsPerformanceLevel', (183, 185)),
                ('controlStatus', (185 + 10, 197 + 10)),
                ('emissionDataLevel', (197 + 10, 207 + 10)),
                ('submittalFlag', (207 + 10, 211 + 10)),
                ('tribalCode', (211 + 10, 214 + 10))
            )
        }

    def _fixedwidth_to_df(self, variable_list, path):
        names = [x[0] for x in variable_list]
        colspecs = [x[1] for x in variable_list]
        df = pd.read_fwf(path, header=None, colspecs=colspecs, names=names)
        return df

    def getTable(self, table, statefips=None, check_for_pickle=True):

        filepath = self.filepath.format(table)

        if check_for_pickle:
            try:
                pickle_name = self.filepath.format(table).replace('txt', 'p')
                return pd.read_pickle(pickle_name)
            except IOError:
                pass
        else:
            pass

        variable_dict = self.column_maps[table]
        df = self._fixedwidth_to_df(variable_dict, filepath)
        df = df[df['recordType'] == table.upper()]
        if statefips:
            df = df[df['fips_state'] == statefips]
        return df

    def getAll(self):
        tables = dict()
        for table in self.column_maps.keys():
            tables[table] = self.getTable(table)
        return tables

    def to_file(self, filetype, statefips=None):
        for table in self.column_maps.keys():
            df = self.getTable(table, statefips=statefips,
                               check_for_pickle=False)
            filepath = self.filepath.format(table).replace('txt', filetype)
            if filetype == 'csv':
                df.to_csv(filepath, index=False)
            elif filetype == 'p':
                df.to_pickle(filepath)
            elif filetype == 'dta':
                df.to_stata(filepath, write_index=False)


class StackPanel(object):

    def __init__(self, new_addmatch=True):

        self.new_adds = new_addmatch

        self.MANUAL_MATCHES = {
            1999: {
                '19_SC_SC_800075': ['191026800075'],    # Chosen interactively
                '19_SC_SC_112853': ['19102675373'],     # Looked it up myself
                '19_SC_SC_106325': ['191026106325'],    # Begin non-el, from
                '19_SC_SC_107654': ['191026107654'],    # interact
                '19_SC_SC_107655': ['191026107655'],
                '19_SC_SC_108113': ['191026108113'],
                '19_SC_SC_11142': ['19102611142'],
                '19_SC_SC_112164': ['191026112164'],
                '19_SC_SC_16737': ['19102616737'],
                '19_SC_SC_3029': ['1910263029'],
                '19_SC_SC_34055': ['19102634055'],
                '19_SC_SC_3968': ['1910263968'],
                '19_SC_SC_45527': ['19102645527'],
                '19_SC_SC_550': ['191026550'],
                '19_SC_SC_5973': ['1910265973'],
                '19_SC_SC_60540': ['19102660540'],
                '19_SC_SC_68122': ['19102668122'],
                '19_SC_SC_7416': ['1910267416'],
                '19_SC_SC_800012': ['191026800012'],
                '19_SC_SC_800144': ['191026111642', '191026800363'],
                '19_SC_SC_84223': ['19102684223'],
                '30_SC_SC_1962': ['3010261962'],
                '30_SC_SC_21887': ['30102621887'],
                '30_SC_SC_40483': ['30102640483'],
                '30_SC_SC_45471': ['30102645471'],
                '36_SC_SC_17956': ['36102617956'],
                '36_SC_SC_18931': ['36102618931'],
                '36_SC_SC_46268': ['36102646268'],
            },
            2002: {
                '36_SC_SC_15872': [np.nan],             # DNE in 2002
                '36_SC_SC_1026': ['361026121737'],      # Address from 1999
                '19_SC_SC_800075': ['191026800075'],    # Chosen interactively
                '19_SC_SC_11142': ['19102611142'],      # Begin non-el interact
                '19_SC_SC_60540': ['19102660540'],
                '19_SC_SC_7416': ['1910267416'],
                '19_SC_SC_800192': ['191026800196'],
                '30_SC_SC_1962': ['3010261962'],
                '30_SC_SC_45471': ['30102645471'],
                '36_SC_SC_46268': ['36102646268'],
            }
        }

        self.UNIFORM_VARNAMES = {
            # Change variables in both (chron order)
            'emissionNumericValue': 'emissions',
            'emissionstpytext': 'emissions',
            'annual_emissions': 'emissions',
            'emissionReleasePointType': 'type_stack',
            'emissionreleasepointtypecode': 'type_stack',
            'emission_release_point_type': 'type_stack',
            'xcoord': 'x',
            'dblxcoordinate': 'x',
            'x_coordinate': 'x',
            'ycoord': 'y',
            'dblycoordinate': 'y',
            'y_coordinate': 'y',
            # make 1999/2005 match 2002
            'SCC': 'scc',                       # 1999
            'emissionUnitID': 'emissionunitid',
            'sic_primary': 'sic',               # 2005
            'stack_height': 'stackheightft',
            'stackdiamft': 'stackdiameterft',   # both
            'stack_diameter': 'stackdiameterft',
            'stackexittempf': 'exitgastemperaturef',
            'exit_gas_temperature': 'exitgastemperaturef',
            'stackexitvelocityfs': 'exitgasvelocityftsec',
            'exit_gas_velocity': 'exitgasvelocityftsec',
        }

    def getstacks(self, firms, year):
        self.datayear = year

        self.manual_matches = self.MANUAL_MATCHES[self.datayear]

        # Load NEI data
        if year == 1999:
            site_info = self.load_nei1999_site()
            stack_table = self.load_nei1999_emissions()
        elif year == 2002:
            site_info, stack_table = self.load_nei2002()

        # Get ufacid-NEI xwalk and conflicts from address match
        nei_xwalk = self.get_nei_xwalk(firms, site_info)

        # Join ufacid and stack data
        ufacid_stacks = pd.merge(nei_xwalk, stack_table,
                                 on='stateID', how='left')
        # Get stacks' SIC
        ufacid_stacks = pd.merge(ufacid_stacks, site_info[['stateID', 'sic']],
                                 on='stateID', how='left')

        return ufacid_stacks

    def get_nei_xwalk(self, firms, site_info):

        # Try direct stateID match
        xwalk = pd.merge(firms, pd.DataFrame(site_info['stateID']),
                         left_on='my_stateID', right_on='stateID', how='left')
        xwalk.index = firms.index

        # Try matching by address
        matched_on_add = self._xwalk_via_address(
            firms, site_info, new=self.new_adds)

        # Join stateID and address matches together
        xwalk = xwalk.join(matched_on_add, how='left')

        # Check for nothing crazy
        self._xwalk_sanity_check(xwalk)

        # Reshape, drop good duplicates
        xwalk.columns.name = 'match_type'
        long_xwalk = xwalk.filter(regex='^stateID|match_*').stack()
        long_xwalk = long_xwalk.drop_duplicates()
        long_xwalk = pd.DataFrame(long_xwalk, columns=['stateID']).reset_index()

        # Merge info onto xwalk
        site_vars_to_keep = ['stateID', 'name', 'street', 'city', 'zip']
        long_xwalk_w_info = pd.merge(long_xwalk, site_info[site_vars_to_keep],
                                     on='stateID', how='left')
        firms_w_full_data = pd.merge(firms, long_xwalk_w_info,
                                     left_index=True, right_on='ufacid',
                                     how='left')

        # Save match info for eyeballing (includes unmatched firms)
        match_file_name = 'tmp_firms_nei_matches_{}.dta'.format(self.datayear)
        firms_w_full_data.to_stata(data_path(match_file_name),
                                   write_index=False)

        return long_xwalk

    def load_nei1999_site(self):

        # Site info table
        site_info = read_rawNEI1999().getTable('si')
        site_info = site_info[site_info['recordType'].notnull()]
        site_info = site_info[site_info['state'] == 'CA']

        site_info['zip'] = site_info['zip'].str.replace('UNKNOWN', '0')
        site_info['zip'] = site_info['zip'].replace(
            '^(\d+)[-]{0,1}.*$', '\g<1>', regex=True)

        site_info.rename(columns=self.UNIFORM_VARNAMES, inplace=True)

        return site_info

    def load_nei1999_emissions(self):
        # Unique, cross-table ID's
        unique_process_id = ['fips_county', 'stateID', 'emissionUnitID',
                             'processID', 'emissionReleasePointID']
        unique_stack_id = ['fips_county', 'stateID', 'emissionReleasePointID']

        # Release point table
        stack_table = read_rawNEI1999().getTable('er', statefips=6)
        # Emissions process table
        ep_table = read_rawNEI1999().getTable('ep', statefips=6)

        # Emissions table
        emit_table = read_rawNEI1999().getTable('em', statefips=6)
        emit_table = emit_table[emit_table['pollutantCode'] == 'NOX']
        # Keep only longest duration
        for date in ['startDate', 'endDate']:
            emit_table[date] = pd.to_datetime(
                emit_table[date].astype(str), format='%Y%m%d')
        emit_table['duration'] = emit_table['endDate'] - emit_table['startDate']
        # Convert to float (Days)
        emit_table['duration'] = emit_table['duration'].astype('timedelta64[D]')
        # Keep only the last one
        emit_table.sort_values(unique_process_id + ['duration'], inplace=False)
        emit_table = emit_table.groupby(unique_process_id).last().reset_index()
        # Make sure emissions records are now unique
        assert emit_table.groupby(unique_process_id).size().min() == 1

        # Keep relevant non-ID variables
        STACK_VARS = ['stackheightft', 'stackdiamft', 'stackexittempf',
                      'stackexitvelocityfs',
                      'emissionReleasePointType',
                      'xcoord', 'ycoord']

        EMISSION_VARS = ['emissionNumericValue', 'emissionUnitNumerator',
                         'emissionCalculationMethodCode']

        PROCESS_VARS = ['SCC']

        stacks = stack_table[unique_stack_id + STACK_VARS].copy()
        emissions = emit_table[unique_process_id + EMISSION_VARS].copy()
        processes = ep_table[unique_process_id + PROCESS_VARS].copy()

        # Merge up to master stack data set (only stacks with NOX)
        # stack have processes, which have emissions
        process_emissions = pd.merge(emissions, processes, on=unique_process_id,
                                     how='left')  # We've restricted emissions

        stack_data = stata_merge(process_emissions, stacks, on=unique_stack_id,
                                 how='left', assertval=3)

        stack_data.rename(columns=self.UNIFORM_VARNAMES, inplace=True)

        return stack_data

    def load_nei2002(self, alldata=False):
        nei2002path = path.join(NEI_PATH, '2002', 'nei_nox_ca_2002_plus.dta')
        # Read NEI data, clean up
        nei = pd.read_stata(nei2002path)
        renamevars = {'facilityname': 'name',
                      'statefacilityid': 'stateID',
                      'siccode': 'sic',
                      'locationaddress': 'street',
                      'zipcode': 'zip',
                      'emisssionunitid': 'emissionunitid'}
        nei = nei.rename(columns=renamevars)

        # State restriction made it data itself, plus a few others for matching
        # nei = nei[nei['state'] == 'CA']

        # Restrict to only NOX sites/stacks
        nei = nei[nei['pollutantcode'] == 'NOX']

        if alldata:
            nei.rename(columns=self.UNIFORM_VARNAMES, inplace=True)
            return nei

        # Make address table
        address_vars = ['name', 'street', 'city', 'zip', 'sic']
        nei_site_info = nei[['stateID'] + address_vars]
        nei_site_info = nei_site_info.drop_duplicates()
        nei_site_info['state'] = 'CA'

        # Make stack table
        stack_vars_2002 = [
            'stackheightft', 'stackdiameterft', 'exitgastemperaturef',
            'exitgasvelocityftsec', 'emissionstpytext',
            'scc', 'emissionreleasepointtypecode',
            'emissionunitid', 'processid', 'emissionreleasepointid',
            'dblxcoordinate', 'dblycoordinate']
        stack_data = nei[['stateID'] + stack_vars_2002]

        stack_data = stack_data.rename(columns=self.UNIFORM_VARNAMES)

        return nei_site_info, stack_data

    def load_nei2005(self):
        path_2005 = path.join(NEI_PATH, '2005', 'nei_ca_nox_2005.dta')
        nei = pd.read_stata(path_2005)
        nei.rename(columns=self.UNIFORM_VARNAMES, inplace=True)

        keep_vars = ['sic', 'scc', 'emissions', 'type_stack'] + STACK_VARS_2002
        nei = nei[keep_vars]

        return nei

    def _xwalk_via_address(self, firmDF, neiDF, new=True):
        add_matched_path = data_path(
            'tmp_firms_nei_add_match{}.p').format(self.datayear)
        if new:
            firms = firmDF.copy()
            nei = neiDF.copy()

            nei = nei.set_index('stateID')

            firms = firms[firms['fstreet'] != '']
            firms['state'] = 'CA'
            firm_args = {'street': 'fstreet', 'city': 'fcity',
                         'state': 'state', 'zipname': 'fzip'}

            interact_record = data_path(
                'tmp_address_match_record_{}.txt'.format(self.datayear))
            address_matches = MatchAdd(firms, nei, left_args=firm_args,
                                       nozip=True,
                                       manual_matches=self.manual_matches,
                                       interact_record=interact_record)
            address_matches.to_pickle(add_matched_path)
        else:
            try:
                address_matches = pd.read_pickle(add_matched_path)
            # If the file doesn't exist, override 'new', try again
            except IOError:
                return self._xwalk_via_address(firmDF, neiDF, new=True)

        return address_matches

    def _xwalk_sanity_check(self, xwalk):
        # Flag: Is the manual stateID that matched also matched via addresses?
        no_match_conflict = xwalk.apply(lambda x: (
            x['stateID'] in x.filter(regex='match').tolist())
            or (pd.isnull(x['stateID'])
                or pd.isnull(x['match_1'])), axis=1)
        # After eliminating stateID-address matches (above), are the remaining
        # matches unique?
        unique_xwalk = xwalk.copy()
        unique_xwalk.loc[no_match_conflict, 'stateID'] = np.nan
        stateID_list = unique_xwalk.filter(regex='match|^stateID').stack()
        stateID_list.name = 'list'
        sizes = pd.DataFrame(stateID_list).groupby('list').size()
        assert (sizes.max() == 1) or (sizes.empty)

    def floaters(self):
        check_dict = {
            'emissionUnitNumerator': set([np.nan, 'TON']),
            'emissionCalculationMethodCode': set([np.nan]),
            'emissionReleasePointType': set([1, 2]),
        }
        wut = pd.DataFrame()
        # Sanity checks for stack/emissions data
        # Check that I'm anticipating all the category types
        assert (set(wut['emissionUnitNumerator'].unique())
                <= set([np.nan, 'TON']))
        return check_dict


def clean_ufacids_stacks(stacks, nei_year=2002,
                         impute=True, impute_band=.05):
    ufacid_stacks = stacks.copy()
    # Drop small-frys
    ufacid_stacks = _drop_smallfrys(ufacid_stacks)
    # Drop all fugitives if >90% vertical
    fug_share = calc_fugitive_share(ufacid_stacks)
    meet_cutoff = fug_share[fug_share < 10].index
    drop_fugs = np.logical_and(
        ufacid_stacks['ufacid'].isin(meet_cutoff),
        ufacid_stacks['type_stack'] == 1
    )
    ufacid_stacks = ufacid_stacks[~drop_fugs]

    # Impute remaining fugitives
    if nei_year == 2002:
        full_nei = StackPanel().load_nei2002(alldata=True)
        full_nei = full_nei[
            ['sic', 'scc', 'emissions', 'type_stack']
            + STACK_VARS_2002
        ]
    elif nei_year == 2005:
        full_nei = StackPanel().load_nei2005()

    if impute:
        ufacid_stacks = ufacid_stacks.apply(_impute_fugs, axis=1,
                                            args=(full_nei, impute_band))
    else:
        pass

    return ufacid_stacks

def _drop_smallfrys(ufacid_stacks): #noqa
    stacks = ufacid_stacks.copy()

    output = 'emissions'
    # Calc ufacid's total emissions
    ufacid_sum = stacks.groupby('ufacid', as_index=False)
    ufacid_sum = ufacid_sum[output].sum()
    ufacid_sum.rename(columns={output: 'ufacid_total'}, inplace=True)
    # Get firm's ssc's total fugitive emissions
    is_fugitive = stacks['type_stack'] == 1
    group_sum = stacks[is_fugitive].groupby(['ufacid', 'scc'], as_index=False)
    group_sum = group_sum[output].sum()
    group_sum.rename(columns={output: 'group_total'}, inplace=True)

    sums = pd.merge(ufacid_sum, group_sum, on='ufacid')

    stacks = pd.merge(stacks, sums, on=['ufacid', 'scc'], how='left')

    flags = []
    flags.append(stacks['group_total'] < 1.5)
    flags.append(stacks['group_total']/stacks['ufacid_total'] < .1)
    # flags.append(flag_cap_level = stacks['group_total'] < 10)
    flags.append(stacks['type_stack'] == 1)

    small_fry = pd.concat(flags, axis=1).all(axis=1)

    pared_stacks = stacks[~small_fry]

    return pared_stacks[ufacid_stacks.columns]

def _impute_fugs(stackrow, nei, output_bandwidth):  #noqa
    if stackrow['type_stack'] == 2:
        return stackrow
    else:
        output = 'emissions'
        lo_emit = stackrow[output] * (1. - output_bandwidth)
        hi_emit = stackrow[output] * (1. + output_bandwidth)
        match_sic = nei['sic'] == stackrow['sic']
        match_scc = nei['scc'] == stackrow['scc']
        match_output = np.logical_and(lo_emit < nei[output],
                                      nei[output] < hi_emit)
        is_vertical = nei['type_stack'] == 2
        match = pd.concat(
            [match_sic, match_scc, match_output, is_vertical], axis=1)
        nei_sample = match.all(axis=1)
        # If no matches, don't require output match
        if not nei_sample.max():
            sub_match = match.filter(regex='[^{}]'.format(output))
            nei_sample = sub_match.all(axis=1)
            # If still no match, don't require sic match
            if not nei_sample.max():
                del sub_match['sic']
                nei_sample = sub_match.all(axis=1)
                # If still no match, just drop
                if not nei_sample.max():
                    stackrow[STACK_VARS_2002] = np.nan
                    return stackrow

        samp_grouper = nei[nei_sample].groupby(STACK_VARS_2002, as_index=False)
        samp_totals = samp_grouper[output].sum().sort_values(output)

        new_stack_vars = samp_totals.iloc[-1][STACK_VARS_2002]
        stackrow.update(new_stack_vars)
        stackrow['type_stack'] = 2
        return stackrow

def calc_fugitive_share(df, fug_only=True):  #noqa

    df.rename(columns={'SCC': 'scc'}, inplace=True)

    grouper = df.groupby(['ufacid', 'type_stack', 'scc'])
    summed_by_type = pd.DataFrame(grouper['emissions'].sum())
    summed_by_type.columns = ['type_sum']
    total = summed_by_type.groupby(level='ufacid').sum()
    total.columns = ['total']
    shares = summed_by_type.join(total)
    shares['type_share'] = 100*shares['type_sum']/shares['total']
    del shares['total']

    shares.index.names = ['ufacid', 'type', 'scc']

    if fug_only:
        shares = shares.groupby(level=['ufacid', 'type']).sum()
        shares = shares.xs(1, level='type')['type_share'].fillna(0)

    return shares
