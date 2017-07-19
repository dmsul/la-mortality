import pandas as pd
from fuzzywuzzy import fuzz

from econtools import load_or_build, confirmer, DataInteractModel

from util.system import data_path
from util.gis import code_series, convert_utm, BAD_LOC_TYPES, cleanadds
from clean.pr2.rawio import read_pr2table

safe_geocoding_dir = 'geocode'


@load_or_build(data_path(safe_geocoding_dir, 'pr2_geocodes_clean.p'))
def load_pr2geocodes():

    adds = get_cleaned_adds()
    geocodes = load_raw_geocodes(adds)
    # Add UTM, because why not
    geocodes = geocodes.rename(columns={'lat': 'y', 'lon': 'x'})
    geocodes['utm_east'], geocodes['utm_north'] = convert_utm(geocodes)
    # Eyeball check of raw geocodes (saves failed to DTA file)
    passed = eyeball_check_geocodes(geocodes)
    # Merge back original source addresses used for geocoding
    outdf = pd.merge(adds, passed, on='address', how='inner')

    outdf = outdf.set_index('facid')
    return outdf


def get_cleaned_adds():
    adds = read_pr2table('rtc_address', clean=True)

    # Clean addresses
    cleaned = cleanadds(adds, outname='address', nozip=True).reset_index()

    # Address changed, '692' no longer makes sense
    _address_correct(cleaned, '692( STUDEBAKER)', '690 N\g<1>')

    _address_correct(cleaned, '936-1000( BARRACUDA)', '936\g<1>')
    _address_correct(cleaned, 'TERMINAL ISLAND', 'SAN PEDRO')
    _address_correct(cleaned, 'MCAS( EL TORO)', 'MARINE CORPS AIR STATION\g<1>')
    _address_correct(cleaned, '(EL TORO, )SANTA ANA', '\g<1>IRVINE')
    _address_correct(cleaned, '5151 & 5401( ALCOA)', '5151\g<1>')
    _address_correct(cleaned, '5148 & 5164( ALCOA)', '5148\g<1>')
    _address_correct(cleaned, '(12459 ARROW )HWY', '\g<1>ROUTE')
    _address_correct(cleaned, '1 NORTHROP AVE/(12250 CRENSHAW)', '\g<1>')
    _address_correct(cleaned, '(FOSTER RD) & CARMINITA', '\g<1>')
    _address_correct(cleaned, '200 SITE( DR, BREA)', '2000 SITE\g<1>')
    _address_correct(cleaned, '(3301) 1700', '\g<1>')
    _address_correct(cleaned, '(AVIATION) & ROSECRANS', '\g<1>')
    _address_correct(cleaned, '5412 & (5420 E LA PALMA)', '\g<1>')
    _address_correct(cleaned, '(935 E ARTESIA) & 1005 E ARTESIA', '\g<1>')
    _address_correct(cleaned, '^(14741)- 14641', '\g<1>')
    _address_correct(cleaned, '^(3250) 3376( E 44)', '\g<1>\g<2>')
    _address_correct(cleaned, '^(505 PIER B )AV(, WILMING)', '\g<1>ST\g<2>')
    _address_correct(cleaned, '^(2211 E CARSON ST, )LONG BEACH', '\g<1>CARSON')
    _address_correct(cleaned, '^(14104)-14110( TOWNE AV)', '\g<1>\g<2>')
    _address_correct(cleaned, '^(200 )E( RD, LA HABRA)', '\g<1>EAST\g<2>')
    _address_correct(cleaned, '^(200 EAST RD, LA HABRA)(, CA)',
                     '\g<1> HEIGHTS\g<2>')
    _address_correct(cleaned, '(376 )S (VALENCIA, BREA)', '\g<1>\g<2>')
    _address_correct(cleaned, '(331 )& 333 (N VINELAND AV)', '\g<1>\g<2>')
    _address_correct(cleaned, '(20101 )GOLDEN W', '\g<1>GOLDENWEST')
    _address_correct(cleaned, '(LONG BEACH NAVAL )(COMPLX|SHIPYARD)',
                     '\g<1>COMPLEX')

    return cleaned

def _address_correct(df, patt, rep):    #noqa
    col = 'address'
    df[col] = df[col].replace(patt, rep, regex=True)


def load_raw_geocodes(adds):
    # Make address/temp id unique
    unique_adds = adds[['address']].drop_duplicates().reset_index(drop=True)
    # Load/gen raw geocodes via direct call to `code_series`
    # NOTE: Because of the API cost of geocoding (and apparently irregularities
    #       on Google's side, there is no `_rebuild` arg here.
    raw_geocode_storage = data_path(safe_geocoding_dir, 'pr2_geocodes_raw.p')

    @load_or_build(raw_geocode_storage)
    def _load_raw_geocodes(unique_adds):
        df = code_series(unique_adds)
        return df

    return _load_raw_geocodes(unique_adds)


def eyeball_check_geocodes(geocodes):
    """Eyeball geocodes if needed. Save failed matches."""

    bad_add_match = fuzzymatch(geocodes['address'], geocodes['match_address'])
    bad_location_type = geocodes['location_type'].isin(BAD_LOC_TYPES)
    eyeball = (bad_add_match) | (bad_location_type)

    # Manually approve
    approve_adds = []
    #   Short road, close enough for now
    approve_adds.append('11100 Constitution Av, Los Angeles, CA')
    #   Short raod, close enough for now
    approve_adds.append('1175 Carrack Av, Wilmington, CA')
    #   Actually correct
    approve_adds.append('1 Space Park, Redondo Beach, CA')
    approve_adds.append('March ARB, Riverside, CA')
    approve_adds.append('March AFB, Riverside, CA')
    approve_adds.append('Marine Corps Air Station El Toro, Irvine, CA')
    approve_adds.append('LONG BEACH NAVAL COMPLEX, LONG BEACH, CA')
    approve_adds = map(lambda x: x.upper(), approve_adds)

    override_approve = geocodes[geocodes['address'].isin(approve_adds)].index
    eyeball[override_approve] = False

    # Manually fail
    fail_regex = []
    fail_regex.append('Del Valle Oil Field')
    fail_regex.append('Platform')
    fail_regex.append('San Clemente Island, San Clemente, CA')
    fail_regex.append('Redhill/Valencia, Tustin')
    fail_regex.append('Imperial Maple Nash & Selby')
    fail_regex.append('Jurupa & Payton Av')
    fail_regex.append('Pad Windmill Rd, Tustin')
    fail_regex.append('OCS Lease Parcels, Huntington')
    fail_regex.append('N of HWY 126')
    fail_regex.append('OAT Mountain/Sect 19')
    fail_regex.append('^Santa Fe Springs Rd, Santa Fe Springs')
    fail_regex.append('^E Crenshaw 1/2 mile S')
    fail_regex.append('^Belmont Island WTR PRC 186 St')
    fail_regex = map(lambda x: x.upper(), fail_regex)
    override_fail = []
    for patt in fail_regex:
        tmp_fail = geocodes['address'].str.contains(patt, regex=True)
        override_fail += geocodes[tmp_fail.fillna(False)].index.tolist()
    override_fail = list(set(override_fail))

    failed = geocodes.loc[override_fail, :]
    geocodes = geocodes.drop(override_fail)  # Drop failed rows before eyeball
    eyeball = eyeball.drop(override_fail)

    passed, to_eyeball = geocodes[~eyeball], geocodes[eyeball]

    passed_eyeball = GeocodeCheck(to_eyeball).interact()

    passed = passed.append(to_eyeball[passed_eyeball])
    failed = failed.append(to_eyeball[~passed_eyeball])

    failed_storage = data_path(safe_geocoding_dir, 'pr2_geocodes_failed.dta')
    failed.to_stata(failed_storage)

    return passed

def fuzzymatch(inadd, outadd):  #noqa
    """Outer-product map of `fuzz.ratio` for two address Series."""
    outadd = outadd.apply(_std_match_add)
    fuzz_ratio = pd.DataFrame(inadd).apply(
        lambda x: fuzz.ratio(x['address'], outadd[x.name]), axis=1)
    not_match = fuzz_ratio < .7
    return not_match

def _std_match_add(address):    #noqa
    """Standardize 'matched' (from Google) address for fuzzy matching."""
    parts = address.upper().split(',')
    std_street = parts[0]
    std_address = ','.join([std_street] + parts[1:-1])
    return std_address


class GeocodeCheck(DataInteractModel):

    def interact(self):
        responses = []
        while self.looplist:
            result = self.display(self.looplist.pop())
            if result is not None:
                responses.append(result)
        responses_df = pd.concat(responses)
        return responses_df

    def display(self, row):
        print row['address']
        print row['match_address']
        ans = confirmer('A Match?')

        return pd.Series({row.name: ans})


if __name__ == '__main__':
    df = load_pr2geocodes()
