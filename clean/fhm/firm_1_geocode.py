from os import path, stat
import re
import time
import argparse
import pandas as pd
import numpy as np

from geopy import geocoders
from geopy.distance import vincenty
from geopy.exc import GeocoderTimedOut

from util.system import data_path
from fhm.rawio import raw_firm_data

################

# Uniform street-type abbreviations
St_abbrev = {'clean_street': {' STREET': ' ST',
                              ' ROAD': ' RD',
                              ' AVENUE': ' AV',
                              ' AVE': ' AV',
                              ' HIGHWAY': ' HWY',
                              ' DRIVE': ' DR',
                              ' PLACE': ' PL',
                              ' BLVD': ' BL',
                              ' BOULEVARD': ' BL',
                              ' COURT': ' CT',
                              ' NORTH': ' N',
                              ' SOUTH': ' S',
                              ' EAST': ' E',
                              ' WEST': ' W '}}
match_end = r'(\s+|$)'  # The st type is followed by a space or EOL
keep_end = r'\g<1>'  # Keep the trailing space if it's there
tempdict = dict()
for key, item in St_abbrev['clean_street'].iteritems():
    tempdict[key + match_end] = item + keep_end
St_abbrev['clean_street'] = tempdict

# These losers sneak through. Don't let them.
Straight_to_losers = ('19_SC_SC_800263',  # Address is 'BUILDING'
                      )


BAD_loc_types = ['APPROXIMATE', 'GEOMETRIC_CENTER']


class ProcFHMAdds(object):

    def Clean(self):
        self.fhm_data = self._clean_FHM_addresses()
        self.fhm_data_2 = self._ad_hoc_clean(self.fhm_data)
        return self._calc_modality(self.fhm_data)

    def _clean_FHM_addresses(self):
        # TODO replace this with util/addressmatcher(handler)
        fhmDF = raw_firm_data()

        # Keep only Reclaim guys with an address
        good_ads = np.logical_and(
            fhmDF['RECLAIM1'] == 1, fhmDF['fstreet'] != '')
        fhmDF = fhmDF[good_ads]

        # Make crosswalk of cleaned stuff
        fvars = ['fstreet', 'fcity', 'fzip']
        clean_vars = [x.replace('f', 'clean_') for x in fvars]
        for idx, var in enumerate(fvars):
            fhmDF[clean_vars[idx]] = fhmDF[var]

        # Make missing Zips 0 to place nice later
        fhmDF.loc[fhmDF['clean_zip'].isnull(), 'clean_zip'] = 0
        fhmDF['clean_zip'] = fhmDF['clean_zip'].astype(int).astype(str)
        fhmDF.loc[fhmDF['clean_zip'] == '0', 'clean_zip'] = ''

        # Make all street types abbreviated
        fhmDF = fhmDF.replace(St_abbrev, regex=True)

        # Remove commas, apostrophes, extra spaces
        re_matches = ["\.", "'", "\s\s+"]
        for regex in re_matches:
            fhmDF['clean_street'] = fhmDF['clean_street'].replace(
                regex, ' ', regex=True)
        fhmDF['clean_street'] = fhmDF['clean_street'].str.strip()

        fhmDF['clean_address'] = fhmDF.apply(_join_address, axis=1)
        fhmDF = fhmDF[['ufacid', 'clean_address']]

        return fhmDF

    def _calc_modality(self, df):
        pair_count = pd.DataFrame(
            df.groupby(['ufacid', 'clean_address']).size(),
            columns=['pair_count'])
        facid_count = pd.Series(df.groupby(['ufacid']).size(), name='total')
        modality = pair_count.join(facid_count)
        modality['share'] = modality['pair_count'] / modality['total']
        del modality['pair_count']

        # get unique ufacid-clean_address pairs and their counts
        # calculate pair's w/in ufacid share
        modality = modality.reset_index().sort_values(
            ['ufacid', 'share'], ascending=False)
        return modality.reset_index(drop=True)

    def _ad_hoc_clean(self, df):
        """Manual adjustments to get a few more geocoding results"""

        # 30_SC_SC_800069, 19_SC_SC_9755: Keep street before '&'
        this_ufacid = df.ufacid == '19_SC_SC_9755'
        df.loc[this_ufacid, 'clean_address'] = (
            df.loc[this_ufacid, 'clean_address'].replace(
                '& 6020 ', '', regex=True))
        this_ufacid = df.ufacid == '30_SC_SC_800069'
        df.loc[this_ufacid, 'clean_address'] = (
            df.loc[this_ufacid, 'clean_address'].replace(
                '^(.*) &.*T(, F)', '\g<1>\g<2>', regex=True))

        # Change city "Terminal island" to "San Pedro"
        df['clean_address'] = df['clean_address'].replace(
            'TERMINAL ISLAND', 'SAN PEDRO', regex=True)

        # replace '(.* PIER .*)(BERTH [0-9]+)' with '\g<1>'
        df['clean_address'] = df['clean_address'].replace(
            '(.* PIER.*) BERTH [0-9]+(.*|$)', '\g<1>\g<2>', regex=True)

def _join_address(x):   #noqa
    skelstr = '{}, {}, {}{}'
    zipcode = ' ' + x['clean_zip'] if x['clean_zip'] else ''
    return skelstr.format(x['clean_street'], x['clean_city'], 'CA', zipcode)


def _centroid(df):
    centroid = (df['lat'].mean(), df['lon'].mean())
    d_centroid = pd.Series(-1 * np.ones(df.shape[0]), index=df.index,
                           name='d_centroid')
    for idx, row in df.iterrows():
        d_centroid[idx] = vincenty(centroid, tuple(row[['lat', 'lon']])).meters
    return d_centroid, centroid


class MetaGeocoder(object):

    def __init__(self):
        geocode_path = data_path('geocode')
        self.csv_path = {
            'win': path.join(geocode_path, '{}winners.csv'),
            'weird': path.join(geocode_path, '{}weirdos.csv'),
            'lost': path.join(geocode_path, '{}losers.csv')
        }
        self.fileinfix = 'raw_'

    def GetGeocodes(self, df):
        self.googlecoder = geocoders.GoogleV3(
            api_key='AIzaSyAScRWTK6tB2rNPmFZUidym-IlfULn2mM0')
        winnerDF = pd.DataFrame()
        weirdDF = pd.DataFrame()
        loserDF = pd.DataFrame()

        grouped = df.groupby('ufacid')

        for ufacid, addressDF in grouped:
            ufacidDF, is_winner = self._geocode_ufacid(addressDF)
            ufacidDF['ufacid'] = ufacid
            if is_winner:
                winnerDF = winnerDF.append(ufacidDF)
            elif not ufacidDF.empty:
                weirdDF = weirdDF.append(ufacidDF)
            else:
                loserDF = loserDF.append(addressDF)

        self.win, self.weird, self.lost = winnerDF, weirdDF, loserDF
        assert not self.win['location_type'].isin(BAD_loc_types).max()
        assert self.win.groupby('ufacid').size().max() == 1

    def _geocode_ufacid(self, srcDF):
        ufacidDF = pd.DataFrame()
        self.zip_re = re.compile(' [0-9]{5}$')

        for idx, row in srcDF.iterrows():
            result_series = pd.Series()
            # Get geocode if possible
            print "Geocoding {}, {}".format(row['ufacid'], idx)
            try:
                result = self._call_google(row['clean_address'])
            except GeocoderTimedOut:
                result = self._call_google(row['clean_address'])

            if result is None:
                pass
            else:
                result_series = self._extract_geo_results(result)
                result_series['clean_address'] = row['clean_address']
                result_series['share'] = row['share']
                ufacidDF = ufacidDF.append(result_series, ignore_index=True)

            # Return it if it's a winner
            a_loser = (result_series.empty
                       or row['ufacid'] in Straight_to_losers
                       or result_series['location_type'] in BAD_loc_types)

            if a_loser:
                continue
            elif row['share'] > .7:
                return ufacidDF, True
            else:
                continue

        return ufacidDF, False

    def _call_google(self, address):
        time.sleep(0.3)
        result = self.googlecoder.geocode(address, exactly_one=True)

        if result is None:
            result = self._call_again(address)
        elif result.raw['geometry']['location_type'] in BAD_loc_types:
            result = self._call_again(address)
        else:
            pass

        return result

    def _call_again(self, address):
        print "Trying again"
        without_zip = self.zip_re.sub('', address)
        return self.googlecoder.geocode(without_zip, exactly_one=True)

    def _extract_geo_results(self, georesult):
        d = dict()
        d['match_address'] = georesult.address
        d['lat'] = georesult.latitude
        d['lon'] = georesult.longitude
        d['match_type'] = georesult.raw['types'][0]
        d['location_type'] = georesult.raw['geometry']['location_type']
        return pd.Series(d)

    def ReadOldGeocodes(self):
        tmp = dict()
        for key in self.csv_path.keys():
            fname = self.csv_path[key].format(self.fileinfix)
            fstat = stat(fname)
            # CSV might be empty (esp 'losers'), check for it
            if fstat.st_size > 10:
                tmp[key] = pd.read_csv(fname)
            else:
                tmp[key] = pd.DataFrame()

        self.__dict__.update(tmp)

    def SaveGeocodes(self):
        for key in self.csv_path.keys():
            self.__dict__[key].to_csv(
                self.csv_path[key].format(self.fileinfix), index=False)

        # Google returns unicode strings. Pandas can't print ascii and unicode,
        # so convert to ascii
        self.win['match_type'] = self.win['match_type'].str.encode('ascii')
        self.win.to_stata(
            self.csv_path['win'].format(self.fileinfix).replace(
                'csv', 'dta'), write_index=False)


class PPInteract(object):

    def __init__(self, df):
        self.df = df
        self.tmpDF = df.copy()

    def _handle_response(self, choice):
        idx = self.df.index.values

        if choice == 's':
            ans = raw_input('>>> Subset or start [o]ver: ')
            if ans == 'o':
                return self.get_response()
            else:
                ans = ans.split(' ')
                try:
                    ans = [int(x) for x in ans]
                except ValueError:
                    print 'You typed an index wrong, starting over!'
                    return self.get_response()

            if set(ans) <= set(idx):
                subset = ans
                match_type = 'subset_centroid'
            else:
                print 'Bad choice, starting over'
                return self.get_response()

        elif choice == 'c':
            subset = idx
            match_type = 'centroid'

        elif choice.isdigit():
            subset = [int(choice)]
            match_type = 'chosen'

        # Now calc centroid of relevant subset, return one row
        retDF = self.df.loc[subset, :].iloc[0, :]
        _, cent = _centroid(self.df.loc[subset, :])
        retDF['lat'], retDF['lon'] = cent
        retDF['match_type'] = match_type
        retDF['interact_notes'] = raw_input('\nNotes about choice >>> ')
        return retDF

    def _prompter(self, ans=None):
        warn_str = 'Invalid response, try again.\n\n'
        main_str = ('Options:\n'
                    'Use row [#]\n'
                    'Use [c]entroid\n'
                    'Use centroid of [s]ubset\n'
                    'Add to [l]osers list\n'
                    '[d]isplay from beginning again\n'
                    '>>> ')
        if ans is None:
            prompt = main_str
        else:
            prompt = warn_str + main_str

        return raw_input(prompt)

    def get_response(self):

        df = self.df.copy()
        df['d_cent'], centroid = _centroid(df)
        summ_str = ('------------\n'
                    'Ufacid {}, centroid is {}, {}\n'.format(
                        df['ufacid'].iloc[0], *centroid) +
                    '------------\n')
        row_str = ("Row: {idx}\n"
                   "\t{clean_address}\n"
                   "\t{match_address}\n"
                   "\t{location_type}\t{match_type}\tShare: {matchs_share}\n"
                   "\tD_centroid: {d_cent}\tLat: {lat}\tLon: {lon}\n")

        for idx, row in df.iterrows():
            summ_str += row_str.format(idx=idx, **row.to_dict())

        print summ_str

        index_tuple = tuple(self.df.index.astype(str).tolist())
        acceptable = ('c', 's', 'd', 'l') + index_tuple

        answer = None
        while answer not in acceptable:
            answer = self._prompter(answer)

        if answer == 'd':
            return self.get_response()
        elif answer == 'l':
            return pd.DataFrame()
        else:
            return self._handle_response(choice=answer)


class PostProcessor(object):

    MAX_D_CENTROID = 150.

    def __init__(self, MetaCoder):
        for key, item in MetaCoder.__dict__.iteritems():
            if isinstance(item, pd.DataFrame):
                self.__dict__[key] = item.copy()
            else:
                self.__dict__[key] = item
        self.csv_path['interactDF'] = self.csv_path['win'].replace(
            'winners', 'interacted')
        self.interactDF = pd.DataFrame()
        self.fileinfix = 'cleaned_'

    def _append(self, dfname, to_append):
        self.__dict__[dfname] = self.__dict__[dfname].append(to_append)

    def _handleOddballs(self, df):

        d_cent, cent = _centroid(df)

        is_rooftop = df['location_type'] == 'ROOFTOP'
        num_rooftops = is_rooftop.sum()
        all_close = d_cent.max() <= self.MAX_D_CENTROID

        if all_close and num_rooftops == 1:
            takeDF = df[is_rooftop]
            self._append('win', takeDF)
        elif all_close:
            df['lat'], df['lon'] = cent
            takeDF = df.iloc[0, :].copy()
            takeDF['match_type'] = 'Iaveraged'
            self._append('win', takeDF)
        else:
            self._append('interactDF', df)

    def basicpost(self):
        grouped = self.weird.groupby('ufacid')
        for idx, addressDF in grouped:
            tmpDF = addressDF.copy()
            tmpDF['matchs_share'] = tmpDF.groupby(
                ['match_address'])['share'].transform(sum)
            tmpDF = tmpDF.drop_duplicates(['match_address'])
            tmpDF = tmpDF[~tmpDF['location_type'].isin(BAD_loc_types)]
            if tmpDF.empty:
                self._append('lost', addressDF)
            elif tmpDF.shape[0] == 1:
                self._append('win', tmpDF)
            else:
                self._handleOddballs(tmpDF)
        self.weird = pd.DataFrame()

    def interact(self):
        grouped = self.interactDF.groupby('ufacid')
        for ufacid, ufacidDF in grouped:

            if ufacid in Straight_to_losers:
                self._append('lost', ufacidDF)
                continue

            response = PPInteract(ufacidDF).get_response()
            if response.empty:
                self._append('lost', ufacidDF)
            else:
                self._append('win', response)

    def doubleCheckElec(self):
        pass

    def SaveGeocodes(self):
        for key in self.csv_path.keys():
            self.__dict__[key].to_csv(
                self.csv_path[key].format(self.fileinfix), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--geocode', action='store_true',
                        help="Gen new geocodes. Else, read from disk.")
    ARGS = parser.parse_args()

    my_geocoder = MetaGeocoder()
    # Geocode
    if ARGS.geocode:
        adds_with_modal = ProcFHMAdds().Clean()
        my_geocoder.GetGeocodes(adds_with_modal)
        my_geocoder.SaveGeocodes()

    # or Read old codes
    else:
        my_geocoder.ReadOldGeocodes()

    # Pass codes to post-processor
    Post = PostProcessor(my_geocoder)
    Post.basicpost()
    Post.interact()
    Post.SaveGeocodes()
