import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

from util.gis import cleanadds

# TODO use name match to pare down address matches


def _match_series_from_list(match, prefix='match'):
    # Force list
    if hasattr(match, '__iter__'):
        match_list = match
    else:
        match_list = [match]

    if match_list:
        index = ['{}_{}'.format(prefix, i + 1) for i in range(len(match_list))]
    else:
        index = ['{}_1'.format(prefix)]
        match_list = [np.nan]

    return pd.Series(match_list, index=index)


def _get_matches(leftrow, rightDF, minratio):
    rat = rightDF.apply(lambda x: fuzz.ratio(leftrow['clean_address'], x))
    good_matches = rat[rat >= minratio]
    good_matches_sorted_idx = good_matches.sort_values(
        ascending=False, inplace=False).index
    ordered_matches = rightDF[good_matches_sorted_idx].index.tolist()
    return _match_series_from_list(ordered_matches)


class AddMatcher(object):

    def __init__(self, left, right,
                 minratio=95,
                 clean=False, left_clean=True, right_clean=True,
                 left_args=[], right_args=[],
                 nozip=False, restrict_by_name=False,
                 manual_matches=dict(),
                 interact_record='address_match_manual.txt'):

        self.left = left
        self.right = right
        self.minratio = minratio
        self.restrict_by_name = restrict_by_name

        self.manual_matches = manual_matches

        self.interact_record = interact_record
        with open(self.interact_record, 'w') as f:
            f.write("Record of Address Match Selections\n")

        if not left_args:
            left_args = {'street': 'street', 'city': 'city',
                         'state': 'state', 'zipname': 'zip'}
        if not right_args:
            right_args = {'street': 'street', 'city': 'city',
                          'state': 'state', 'zipname': 'zip'}

        if clean:
            left_clean, right_clean = True, True

        if left_clean:
            cleft = cleanadds(self.left, nozip=nozip, **left_args)
        else:
            cleft = self.left

        if not isinstance(cleft, pd.DataFrame):
            cleft = pd.DataFrame(cleft)
        self.cleft = cleft

        if right_clean:
            cright = cleanadds(self.right, nozip=nozip, **right_args)
        else:
            cright = self.right
        self.cright = cright

    def _inter_get_requested_matchrows(self, leftrow):
        raw_row_idx = raw_input('Which rows (separate with spaces)? >>> ')
        iloc_rows = [int(x) - 1 for x in raw_row_idx.split(' ')]

        try:
            newleftrow = leftrow.iloc[iloc_rows]
        except IndexError:
            print "Incorrect index, try again"
            newleftrow = self._inter_get_requested_matchrows(leftrow)
        return newleftrow

    def _inter_parse_row_choice(self):
        ans = raw_input("[A]ll, [N]one, or [S]elect rows >>> ")
        if ans in ['A', 'a']:
            return 'a'
        elif ans in ['N', 'n']:
            return 'n'
        elif ans in ['S', 's']:
            return 's'
        else:
            print 'Try again'
            return self._inter_parse_row_choice()

    def interact_with_duplicates(self, leftrow):
        left_index = leftrow.name
        full_leftrow = self.left.loc[left_index]

        right_matched_info = pd.merge(pd.DataFrame(leftrow), self.right,
                                      left_on=left_index, right_index=True,
                                      how='left')
        right_matched_info.rename(columns={left_index: 'stateID'}, inplace=True)

        # Print everyone's info
        left_str = ("\n------------\n"
                    "{index}\n"
                    "{fname}\n"
                    "{fstreet}\t{fcity}\t{fzip}\n"
                    "------------")
        right_str = ("\nRow: {row}"
                     "\t{name}\n"
                     "\t{street}\t{city}\t{zip}\n"
                     "\tStateID: {stateID}")

        print_str = left_str.format(index=left_index, **full_leftrow.to_dict())
        address_matches = []
        for match, right_info in right_matched_info.iterrows():
            # If non-match, skip
            if not isinstance(right_info['street'], str):
                continue
            # If name's don't match, skip
            name_ratio = fuzz.ratio(full_leftrow['fname'], right_info['name'])
            if name_ratio < 75:
                continue

            print_str += right_str.format(
                row=match.replace('match_', ''), **right_info.to_dict())
            address_matches.append(right_info['stateID'])

        if len(address_matches) == 1:
            new_matches = address_matches
        elif len(address_matches) == 0:
            # TODO: Revisit this. Currently, if address matches but no name
            # matches, skip it altogether. Could put filled out `right_str` in a
            # separate set/array, store the name_ratios, then ''.join the
            # right_str array using either the address matches or all of them as
            # desired. This would also make the address matching easy to turn
            # off.
            new_matches = [np.nan]
        else:
            # Get response
            print print_str
            response = self._inter_parse_row_choice()
            # Use all matches
            if response == 'a':
                new_matches = leftrow[leftrow.notnull()]
            # Use none of the matches
            elif response == 'n':
                new_matches = [np.nan]
            # Use some of the matches
            elif response == 's':
                new_matches = self._inter_get_requested_matchrows(leftrow)

        if isinstance(new_matches, pd.Series):
            new_matches_list = new_matches.tolist()
        else:
            new_matches_list = new_matches

        # Record the selections made to file
        with open(self.interact_record, 'a') as f:
            f.write('{}: {},\n'.format(left_index, new_matches_list.__repr__()))

        return _match_series_from_list(new_matches_list)

    def parse_matches(self, matchrow):
            isnull = matchrow.isnull()
            # Set manually
            if matchrow.name in self.manual_matches:
                return _match_series_from_list(
                    self.manual_matches[matchrow.name])
            # Is non-match
            elif isnull['match_1']:
                return _match_series_from_list(np.nan)
            # Is single match
            elif isnull['match_2']:
                return _match_series_from_list(matchrow['match_1'])
            # Send to interact
            else:
                return self.interact_with_duplicates(matchrow)

    def run(self):
        matches = self.cleft.apply(_get_matches, axis=1,
                                   args=(self.cright, self.minratio))
        cleaned_matches = matches.apply(self.parse_matches, axis=1)
        return cleaned_matches


def MatchAdd(left, right, **kwargs):
    return AddMatcher(left, right, **kwargs).run()
