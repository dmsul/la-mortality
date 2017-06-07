from __future__ import division

import pandas as pd

df = pd.read_pickle('../data/bg_demogs_2000.pkl')
df = df.drop('year', axis=1)

new = pd.DataFrame(index=df.index)

# # Education Groups
educ_total = df.filter(regex='_ed').sum(axis=1)

ed_8less = [
    'female_ed0', 'female_ed4', 'female_ed6', 'female_ed8',
    'male_ed0', 'male_ed4', 'male_ed6', 'male_ed8',
]
new['bg_pct_8th_or_less'] = df[ed_8less].sum(axis=1) / educ_total

ed_9to12 = [
    'female_ed9', 'female_ed10', 'female_ed11', 'female_ed12',
    'male_ed9', 'male_ed10', 'male_ed11', 'male_ed12',
]
new['bg_pct_9th_to_12th'] = df[ed_9to12].sum(axis=1) / educ_total

new['bg_pct_hs_grad'] = (
    df[['female_ed_hs', 'male_ed_hs']].sum(axis=1) / educ_total
)

ed_some_college = [
    'female_ed_coll_1', 'female_ed_coll_nod',
    'male_ed_coll_1', 'male_ed_coll_nod'
]
new['bg_pct_some_coll'] = df[ed_some_college].sum(axis=1) / educ_total

new['bg_pct_assoc_degree'] = (
    df[['female_ed_aa', 'male_ed_aa']].sum(axis=1) / educ_total
)

new['bg_pct_bach_degree'] = (
    df[['female_ed_ba', 'male_ed_ba']].sum(axis=1) / educ_total
)

ed_grad = [
    'female_ed_ma', 'female_ed_jd', 'female_ed_phd',
    'male_ed_ma', 'male_ed_jd', 'male_ed_phd',
]
new['bg_pct_grad_degree'] = df[ed_grad].sum(axis=1) / educ_total

# Housing/Income
new['bg_med_hh_inc'] = df['hhincmed']
new['bg_per_capita_inc'] = df['incpercap']
new['bg_med_house_value'] = df['hvalue_median']
new['bg_med_gross_rent'] = df['rent_median']
new['bg_pct_owner_occ'] = df['hunit_owner'] / df['hunit']
new['bg_pct_renter_occ'] = df['hunit_renter'] / df['hunit']
new['bg_pct_vacant'] = df['hunit_vacant'] / df['hunit']

# Race
new['bg_pct_black'] = df['race_black'] / df['pop']
# new['bg_pct_white'] = df['race_white'] / df['pop']
new['bg_pct_hispanic'] = df['race_hisp'] / df['pop']

new.to_stata('../data/blockgroup_2000.dta')
