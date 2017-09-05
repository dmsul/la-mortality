import pandas as pd
import numpy as np

from econtools import load_or_build, force_iterable

from util import UTM
from util.system import data_path, src_path
from util.distance import nearestneighbor
from clean.nei import load_nei, load_1999_fullemit
from clean.pr2.rawio import build_int_qcer
from clean.pr2.geocode import load_pr2geocodes
from clean.pr2.firmgroups import group_lists_full, get_grouprep
from clean.pr2.neixwalk import get_nei_id

stack_vars = ['stack_ht_ft', 'stack_diam_ft', 'stack_temp_f',
              'stack_veloc_ftsec']
unique_stack_id = ['fips_state', 'fips_county', 'state_id', 'release_point_id']


def load_stacks(facid=None, **kwargs):
    df = _load_stacks_guts(**kwargs)

    if facid:
        firm_list = force_iterable(facid)
        df = df[df['facid'].isin(firm_list)]

    return df


@load_or_build(data_path('pr2_stacks.p'))
def _load_stacks_guts(_rebuild_down=False):
    """
    Prep raw stacks for use with Aermod.

    1) Calculate each stack-type's share of total emissions.
    2) Convert units to metric.
    3) Merge in UTM, county population, and met_site data.
    4) Convert `facid` to groups rep facid, check that groups are uniquely
        represented.
    """

    # Calculate each stack-type's share of emissions
    stacks = load_raw_stacks(_rebuild=_rebuild_down)
    firm_total = stacks.groupby('facid')['emissions'].sum()
    firm_total.name = 'firm_total'
    stacks = stacks.join(firm_total, on='facid')
    stacks['emit_share'] = stacks['emissions'] / stacks['firm_total']

    keep_vars = ['facid'] + stack_vars
    df = stacks.groupby(keep_vars)['emit_share'].sum().reset_index()

    # Convert from English to metric
    ft_to_m = 0.3048
    df['stack_ht'] = df['stack_ht_ft'] * ft_to_m
    df['stack_diam'] = df['stack_diam_ft'] * ft_to_m
    df['stack_veloc'] = df['stack_veloc_ftsec'] * ft_to_m
    df['stack_temp'] = (df['stack_temp_f'] + 459.67) * 5. / 9
    df.drop(stack_vars, axis=1, inplace=True)

    # Bring in UTM,
    geocodes = load_pr2geocodes()
    df = df.join(geocodes[UTM], on='facid', how='inner')

    # metsite_code, metside_year
    df = _get_metsite(df)

    # pop1990,
    pop1990 = pd.read_stata(src_path('county_pop_1900-90.dta'))
    pop1990 = pop1990[['fips', 'pop1990']]
    df = pd.merge(df, stacks[['facid', 'fips_county']].drop_duplicates(),
                  on='facid', how='left')
    df['fips'] = df['fips_county'].apply(lambda x: '06' + x)
    df = pd.merge(df, pop1990, on='fips', how='left')
    df.drop(['fips', 'fips_county'], axis=1, inplace=True)

    # Convert `facid` to groups rep facid, check for unique representation
    group_lists = group_lists_full()
    grouprep = df['facid'].apply(lambda x: get_grouprep(group_lists.loc[x]))
    assert grouprep.nunique() == df['facid'].nunique()
    df['facid'] = grouprep

    return df

def _get_metsite(df):     #noqa
    # Basic cleaning of metsite data
    metsite = pd.read_csv(data_path('src', 'scaqmd_metsites.csv'))
    metsite['utm_east'] = metsite['utm_ekm'] * 1000
    metsite['utm_north'] = metsite['utm_nkm'] * 1000
    metsite['metsite_year'] = '09'
    metsite.rename(columns={'z': 'metsite_z', 'code': 'metsite_code'},
                   inplace=True)
    metsite.loc[metsite['metsite_code'] == 'rivr', 'metsite_year'] = '07'
    metsite = metsite[['metsite_code', 'metsite_year', 'metsite_z'] + UTM]
    metsite.set_index('metsite_code', inplace=True)

    nearest = nearestneighbor(df[UTM].drop_duplicates(), metsite,
                              return_dist=True)

    df2 = df.join(nearest, on=UTM, how='left')
    df2 = df2.join(metsite.drop(UTM, axis=1), on='metsite_code')

    return df2


@load_or_build(data_path('pr2_stacks_raw.p'), copydta=True)
def load_raw_stacks():

    stack_df = get_stacks()

    # Drop all bad stacks if >90% of emissions are already covered
    stack_df2 = _drop_bad_if_manygood(stack_df)
    # Drop bad stacks that are small and have a small share of total emissions
    stack_df3 = _drop_smallshare(stack_df2)
    # Impute within firm
    stack_df4 = impute_within_firm(stack_df3)
    # Impute remaining missings
    stack_df5 = fullsamp_impute(stack_df4)

    # And one by hand:
    # This should be the only fool not to get imputed. The default flag in 2002
    # is 'national default', but the values are not totally awful. And no
    # other semi-sensible values exist for this scc. So just use their defaults.
    dum_defaults = dict(stack_ht_ft=92,
                        stack_diam_ft=1.9,
                        stack_temp_f=110,
                        stack_veloc_ftsec=40)
    for col in dum_defaults:
        dum_firm_col = (stack_df5['facid'] == 18931) & (stack_df5[col].isnull())
        stack_df5.loc[dum_firm_col, col] = dum_defaults[col]

    return stack_df5

def _drop_bad_if_manygood(df):              #noqa
    """Drop all bad stacks if >90% of emissions are covered by good stacks"""
    df = df.copy()
    df['isgood'] = df['stack_ht_ft'].notnull()
    # Get firm's emissions by bad/good stacks
    type_id = ['facid', 'isgood']
    type_sum = df.groupby(type_id)['emissions'].sum()
    type_sum.name = 'type_sum'
    # Firm's emissions
    firm_sum = type_sum.groupby(level='facid').sum()
    firm_sum.name = 'firm_sum'
    # By stack type and total together
    sums = pd.DataFrame(firm_sum).join(type_sum).reset_index()
    sums['share'] = sums['type_sum'] / sums['firm_sum']
    # Drop
    drop_all_fugs = sums.loc[
        (sums['share'] < .1) &      # This type's share is small
        (~sums['isgood']),          # This type is bad
        'facid'].unique()           # Get `facid`s that meet this criteria
    out_df = df[~(                              # Drop if
        (df['facid'].isin(drop_all_fugs)) &     # flagged `facid`
        (~df['isgood'])                         # and is a bad stack
    )]

    # Assert no 'facid's lost
    assert len(df['facid'].unique()) == len(out_df['facid'].unique())

    return out_df

def _drop_smallshare(df):                   #noqa
    """Drop bad stacks if they have a small share of the firm's emissions"""
    df = df.copy()
    firm_sum = df.groupby('facid')['emissions'].sum()
    firm_sum.name = 'firm_sum'
    df = df.join(firm_sum, on='facid')
    df['share'] = df['emissions'] / df['firm_sum']

    out_df = df[~(                  # Drop if
        (df['share'] < .05) &       # stack's share is < 5%
        (df['emissions'] < .3) &    # and stack emits < .3 tons per year
        (~df['isgood'])             # and it's a bad stack
    )].copy()
    out_df.drop(['share', 'firm_sum'], axis=1, inplace=True)

    return out_df


def impute_within_firm(df):
    # Create firm/ssc ID's
    df2 = _by_emit_match(df)
    # df3 = _by_fuzzy_emit_match(df2)
    return df2  # , df3

def _by_emit_match(df):                     #noqa
    temp_stack_id = ['facid', 'scc', 'emissions']
    emit_stacks = df[temp_stack_id + stack_vars].dropna()
    # If stacks with same emissions have different height etc, use modal
    counts = emit_stacks.groupby(temp_stack_id + stack_vars).size()
    counts.name = 'counts'
    emit_stacks = counts.reset_index().sort_values('counts')
    unique_emit_stacks = emit_stacks.drop_duplicates(temp_stack_id,
                                                     keep='last')
    del unique_emit_stacks['counts']

    df2 = df.copy()
    df2['winfirm_impute'] = False
    N = df2.shape[0]
    unique_emit_stacks.reset_index(drop=True, inplace=True)
    for idx, unique_row in unique_emit_stacks.iterrows():
        match = np.ones(N).astype(bool)
        for col in temp_stack_id:
            match &= df2[col] == unique_row[col]
        match &= df2[stack_vars[0]].isnull()
        for col in stack_vars:
            df2.loc[match, col] = unique_row[col]
        df2.loc[match, 'winfirm_impute'] = True

    return df2

def _by_fuzzy_emit_match(indf):             #noqa
    TOLERANCE = .3
    df = indf.copy()
    bad_stacks = df[df['stack_ht_ft'].isnull()]
    for idx, row in bad_stacks.iterrows():
        firm_scc = df[(df['facid'] == row['facid']) &
                      (df['scc'] == row['scc']) &
                      (df['isgood'])]
        if firm_scc.empty:
            continue

        emit_diff = np.abs(firm_scc['emissions'] - row['emissions'])
        min_diff = emit_diff.min()
        if (min_diff / row['emissions']) > TOLERANCE:
            continue

        matched = firm_scc.loc[(emit_diff == min_diff),
                               ['facid', 'scc'] + stack_vars]
        # XXX Make this median too?
        modal = keep_modal(matched)
        if matched.shape[0] > 1:
            # import ipdb; ipdb.set_trace()  #
            pass
        df.loc[idx, stack_vars] = modal[stack_vars]

    return df


def fullsamp_impute(indf):
    df = indf.copy()
    df['match_flag'] = ''
    match_vars = ['sic', 'scc']

    nei1999 = load_1999_fullemit(CA=False)
    nei1999 = nei1999[nei1999['pollutant_code'] != 'SOX']
    bad_stacks1999 = flag_bad_stacks(nei1999, year=1999)
    nei1999 = nei1999.loc[~bad_stacks1999, :]

    nei2002 = load_nei(2002, CA=False, rebuild=False)
    nei2002 = nei2002[nei2002['pollutant_code'] != 'SOX']
    bad_stacks2002 = flag_bad_stacks(nei2002, year=2002)
    nei2002 = nei2002.loc[~bad_stacks2002, :]

    unique_stacks = df.loc[df[stack_vars[0]].isnull(),
                           match_vars].drop_duplicates()

    for __, missrow in unique_stacks.iterrows():

        for nei_year in (2002, 1999):
            if nei_year == 2002:
                nei = nei2002
            elif nei_year == 1999:
                nei = nei1999

            matches, match_qual1 = _get_nei_matches(missrow, nei)
            if not matches.empty:
                break

        these_miss = ((df['sic'] == missrow['sic']) &
                      (df['scc'] == missrow['scc']) &
                      (df['stack_ht_ft'].isnull()))
        if not matches.empty:
            best_match, match_qual2 = _choose_best_match(missrow, matches)
            # Assign
            for col in stack_vars:
                df.loc[these_miss, col] = best_match[col]
            # Match quality flag
            df.loc[these_miss, 'match_flag'] = '{}{}'.format(match_qual1,
                                                             match_qual2)
        else:
            df.loc[these_miss, 'match_flag'] = '00'
            print "FAILED"

    df['fullsamp_impute'] = ~df['match_flag'].isin(['', '00'])

    return df

def _get_nei_matches(missrow, nei):         #noqa
    emit, scc, sic = missrow[['emissions', 'scc', 'sic']].tolist()
    bad_sic = np.isnan(sic) or sic == 9999

    if bad_sic:
        by_scc_sic = pd.DataFrame()
    else:
        by_scc_sic = nei[(nei['sic'] == sic) & (nei['scc'] == scc)]

    if by_scc_sic.empty:
        first_match = nei[(nei['scc'] == scc)]
        first_match_qual = 1
    else:
        first_match = by_scc_sic
        first_match_qual = 2

    return first_match, first_match_qual

def _choose_best_match(missrow, matches):   #noqa
    matches = matches.copy()
    # stack vars shouldn't vary within `unique_stack_id`. I think.
    matches['nox'] = matches['pollutant_code'] == 'NOX'
    unique_matches = matches.groupby(unique_stack_id + stack_vars)['nox'].max()
    unique_matches = unique_matches.reset_index()

    # If lots of matches and lots of CA, just use CA
    second_match_qual = 0
    N = unique_matches.shape[0]
    numCA = (unique_matches['fips_state'] == 6).sum()
    if (N > 12) and (numCA > 6):
        unique_matches = unique_matches[unique_matches['fips_state'] == 6]
        second_match_qual += 1

    N = unique_matches.shape[0]
    numnox = unique_matches['nox'].sum()
    if (N > 12) and (numnox > 6):
        unique_matches = unique_matches[unique_matches['nox']]
        second_match_qual += 2

    assert unique_matches.set_index(unique_stack_id).index.is_unique

    med = unique_matches[stack_vars].median()

    if np.isnan(med[stack_vars[0]]):
        raise ValueError("Something is very wrong")

    return med, second_match_qual


def get_stacks():
    """
    Match firms' NEI ID's to NOx producing stacks.

    2002 is the preferred source for stacks, then 1999. 2005 is used to fill
    some gaps manually. Stacks that have listed NOx emissions are preferred;
    non-NOx used to fill gaps.

    Several firms are removed from main `stacks` table because their stack data
    is duplicated by another firm in their firm group.
    """

    # Load all raw NEI data
    nei1999_full = load_nei(1999)
    nei2002_full = load_nei(2002)
    nei2005_full = load_nei(2005)

    nei1999_full['nei_year'] = 1999
    nei2002_full['nei_year'] = 2002
    nei2005_full['nei_year'] = 2002     # right now only using 2005 to patch

    nei1999 = nei1999_full[nei1999_full['pollutant_code'] == 'NOX']
    nei2002 = nei2002_full[nei2002_full['pollutant_code'] == 'NOX']
    nei2005 = nei2005_full[nei2005_full['pollutant_code'] == 'NOX']

    keep_vars = ['fips_county', 'state_id', 'emissions', 'pollutant_code',
                 'emission_unit_id', 'process_id', 'scc', 'release_point_id',
                 'release_point_type', 'sic', 'lon', 'lat', 'xy_default_flag',
                 'stack_default_flag', 'nei_year'] + stack_vars

    # My matched NEI ID's
    nei_ids = get_nei_id()
    # Merge in 2002 stacks
    stacks2002 = _merge_listseries(nei_ids, nei2002, keep_vars)
    # Merge in 1999 stacks for those not hit yet
    used_stateids = stacks2002['state_id'].unique()
    stacks1999 = _merge_listseries(nei_ids, nei1999, keep_vars,
                                   exclude=used_stateids)
    stacks = stacks2002.append(stacks1999)

    # ### Manually add some non-NOX stacks for firms who otherwise don't get hit
    #       (`pr3` gives an idea of how many major stacks)

    # One major source, one large VOC in nei1999, common `scc` for `sic`
    a_stack = nei1999_full[(nei1999_full['state_id'] == '19102675373') &
                           (nei1999_full['emission_unit_id'] == '1') &
                           (nei1999_full['process_id'] == '1') &
                           (nei1999_full['release_point_id'] == '11')]
    stacks = _add_manual_stack(stacks, a_stack, 75373, 1)
    # 4 major srcs, 3 VOC, 1 SO2 stacks in nei1999 (all one `scc`)
    a_stack = nei1999_full[(nei1999_full['state_id'] == '36102615872')]
    stacks = _add_manual_stack(stacks, a_stack, 15872, 4)
    # 5 Process stacks, 5 smallish NOX stacks in 2005
    a_stack = nei2005[(nei2005['state_id'] == '331026117140')]
    stacks = _add_manual_stack(stacks, a_stack, 117140, 5)
    # 3 stacks needed,  3 VOC stacks in 1999
    a_stack = nei1999_full[(nei1999_full['state_id'] == '19102658622')]
    stacks = _add_manual_stack(stacks, a_stack, 58622, 3)
    # 3 major stacks needed, 3 large VOC stacks in 1999
    a_stack = nei1999_full[(nei1999_full['state_id'] == '19102661210') &
                           (nei1999_full['scc'] == 30509202)]
    stacks = _add_manual_stack(stacks, a_stack, 61210, 3)
    # 2 major stacks needed, 2 VOC stacks in 1999
    a_stack = nei1999_full[(nei1999_full['state_id'] == '19102650813')]
    stacks = _add_manual_stack(stacks, a_stack, 50813, 2)
    # 6 large stacks (2 process) needed, 7 VOC stacks found in 1999
    a_stack = nei1999_full[(nei1999_full['state_id'] == '19102695524')]
    stacks = _add_manual_stack(stacks, a_stack, 95524, 7)
    # 1 major stack, grab the biggest
    a_stack = nei1999_full[(nei1999_full['state_id'] == '191026800295') &
                           (nei1999_full['scc'] == 10200602)]
    stacks = _add_manual_stack(stacks, a_stack, 800295, 1)

    # ### Manually remove duplicates

    # Remove 800125 (small stacks) in favor of 115394 (big stacks)
    stacks = stacks[stacks['facid'] != 800125]
    # Remove 115778, duplicates 1026
    stacks = stacks[stacks['facid'] != 115778]
    # Remove 101977, duplicates 16299
    stacks = stacks[stacks['facid'] != 101977]
    # Remove 117151, 800204 in favor of 114138
    stacks = stacks[~stacks['facid'].isin([117151, 800204])]
    # Remove 134768, 144791, duplicates 117581
    stacks = stacks[~stacks['facid'].isin([134768, 144791])]
    # 800070 and 800393 are the same firm (use 800393, better coverage)
    stacks = stacks[stacks['facid'] != 800070]
    # 800126 should be removed in favor of 115389's stacks (it has the majors)
    stacks = stacks[stacks['facid'] != 800126]
    # Remove 14052, duplicates 115536
    stacks = stacks[stacks['facid'] != 14052]
    # Remove 83444 in favor of 117290
    stacks = stacks[stacks['facid'] != 83444]
    # Remove 94079 in favor of 118406
    stacks = stacks[stacks['facid'] != 94079]
    # Remove 800391 in favor of 800192
    stacks = stacks[stacks['facid'] != 800391]

    # If a stack's parameters are imputed, set value to missing
    bad_1999 = flag_bad_stacks(stacks, year=1999)
    bad_2002 = flag_bad_stacks(stacks, year=2002)
    bad_stack = (((stacks['nei_year'] == 1999) & (bad_1999)) |
                 ((stacks['nei_year'] == 2002) & (bad_2002)))
    stacks.loc[bad_stack, stack_vars] = np.nan

    stacks.reset_index(drop=True, inplace=True)

    return stacks

def _merge_listseries(s, df, keep_vars, exclude=[]):  #noqa

    outdf = pd.DataFrame()
    for facid, sids in s.iteritems():

        unused_sids = [x for x in sids if x not in exclude]

        if len(unused_sids) == 0:
            continue
        try:
            facids_stacks = df.loc[df['state_id'].isin(unused_sids), keep_vars]
            facids_stacks['facid'] = facid
            outdf = outdf.append(facids_stacks)
        except KeyError:
            pass

    return outdf

def _add_manual_stack(master_stacks, new_stacks, facid, exp_rows): #noqa
    assert len(new_stacks) == exp_rows
    new_stacks_goodcols = new_stacks.loc[:, master_stacks.columns]
    new_stacks_goodcols['facid'] = facid
    master_stacks = master_stacks.append(new_stacks_goodcols)
    return master_stacks


def flag_bad_stacks(df, year=2002):
    """
    `stack_default_flag=33333` should catch all the bad imputed stacks, but it
    doesn't. Usually the pattern is
    height  diam    temp    veloc
    ------  -----   -----   ------
    10      0.003   72      0.0003
    But this doesn't always hold either. Sometimes height=100.
    Use `stack_default_flag` and velocity and diameter.
    """
    bad_ht = df['stack_ht_ft'].isnull()

    if year == 2002:
        bad_veloc = df['stack_veloc_ftsec'] == 0.0003
        bad_diam = df['stack_diam_ft'] == 0.003
        bad_params = (bad_veloc) | (bad_diam)

        bad_flag = df['stack_default_flag'] > 10000
    elif year == 1999:
        bad_veloc = df['stack_veloc_ftsec'] == 0.35
        bad_diam = df['stack_diam_ft'] == 0.33
        bad_temp = df['stack_temp_f'] == 69
        bad_ht = df['stack_ht_ft'] == 33
        bad_params = (bad_veloc) | (bad_diam) | (bad_temp) | (bad_ht)

        bad_flag = np.zeros(df.shape[0]).astype(bool)
    else:
        ValueError

    is_miss = df[stack_vars].isnull().min(axis=1)

    bad_stack = (bad_params) | (bad_flag) | (is_miss)
    return bad_stack


def keep_modal(indf, cols=[]):
    if not cols:
        cols = indf.columns.tolist()

    counts = indf.groupby(cols).size()
    counts.name = 'counts'
    df = indf.join(counts, on=cols).sort_values('counts')
    del df['counts']

    return df.iloc[-1, :]


# For checking coverage after imputation

def catch_duplicates():
    """
    Usually each group should have at most one of its members in `stacks`. If
    that is not the case, this will catch it for eyeballing.

    Corrections are made by removing duplicate stacks manually in `get_stacks`
    method.
    """

    stacks = load_raw_stacks()
    groups = group_lists_full()
    # Have this in the namespace for when `ipdb` gets called
    emi = build_int_qcer().set_index(['facid', 'year', 'mth'])['emi']   #noqa

    bad_groups = []
    for facid in stacks['facid'].unique():
        this_group = groups.loc[facid]
        buddy_stacks = stacks[stacks['facid'].isin(this_group)]
        if len(buddy_stacks['facid'].unique()) > 1:
            if this_group not in bad_groups:
                bad_groups.append(this_group)
    drop_on = ['state_id', 'emissions', 'emission_unit_id',
               'release_point_id'] + stack_vars
    for group in bad_groups:
        weirdo = stacks[stacks['facid'].isin(group)]
        print weirdo.T
        cleaned = weirdo.sort_values('facid').drop_duplicates(drop_on,
                                                              keep='last')
        print cleaned.T
        import ipdb
        ipdb.set_trace()
        print 'wut'


def check_coverage():
    """Copy this into __main__, go to town"""
    # XXX These are loaded just to have them in the namespace?
    nei1999 = load_nei(1999)
    nei2002 = load_nei(2002)
    nei2005 = load_nei(2005)
    old_stacks = pd.read_stata('../data/firms_stacks.dta')
    old_covered = old_stacks['facid'].unique()
    pr2 = build_int_qcer()
    emi = pr2.set_index(['facid', 'year', 'mth'])['emi']

    sids = get_nei_id()
    groups = group_lists_full()
    new_stacks = load_raw_stacks(False)
    new_covered = new_stacks['facid'].unique()
    lost_own_coverage = [x for x in old_covered if x not in new_covered]

    lost_coverage = []
    for fid in lost_own_coverage:
        group = groups.loc[fid]
        has_one = False
        for gid in group:
            if gid in new_covered:
                has_one = True
                continue
        if not has_one:
            lost_coverage.append(fid)
    print lost_coverage

    lost_groups = [a for b in groups.loc[lost_coverage] for a in b]

    inv_groups = groups.apply(lambda x: min(x))
    # Total coverage
    annual = emi.groupby(level=['facid', 'year']).sum()
    max_annual = annual.groupby(level='facid').max()

    wat = pd.DataFrame(columns=['max_ann', 'first_emit', 'last_emit',
                                'not_in_group'])
    for facid, maxann in max_annual.iteritems():
        try:
            group_cov = [x in new_covered for x in groups.loc[facid]]
        except KeyError:
            not_in_group = True
            is_covered = False
        else:
            not_in_group = False
            is_covered = max(group_cov)

        if not is_covered:
            wat.loc[facid, 'max_ann'] = maxann
            this_emi = emi.loc[facid]
            this_emi_nonzero = this_emi[this_emi > 0.001]
            wat.loc[facid, 'first_emit'] = this_emi.index[0]
            wat.loc[facid, 'last_emit'] = this_emi.index[-1]
            wat.loc[facid, 'not_in_group'] = not_in_group

    wat = wat.sort_values('max_ann')
    print wat


def nocover_nei(facid, nei):
    keepvars = ['state_id', 'facid', 'sic', 'scc', 'name', 'street', 'city',
                'release_point_id', 'emissions', 'pollutant_code',
                'stack_default_flag']
    groups = group_lists_full()
    sids = get_nei_id()
    groups_sids = [a for b in sids.loc[groups.loc[facid]].values for a in b]
    tmpdf = nei.loc[nei['state_id'].isin(groups_sids),
                    keepvars + stack_vars].copy()
    return tmpdf


if __name__ == '__main__':
    # check_coverage()
    df = load_stacks()
