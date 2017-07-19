import pandas as pd
import numpy as np

from econtools import load_or_build, DataInteractModel

from util import UTM
from util.system import data_path
from util.distance import getdist
from util.networks import equiv_class
from util.gis import draw_googlemap
from clean.pr2.rawio import read_pr2table, build_int_qcer
from clean.pr2.geocode import load_pr2geocodes


def get_grouprep(firm_group):
    """Groups are identified by the smallest `facid`"""
    return min(firm_group)


@load_or_build(data_path('pr2_group_lists_full.p'))
def group_lists_full():
    """Series of every `facid` with location group as a list"""
    unique_facids = read_pr2table('rtc_address', clean=True).index.unique()

    firms_joingroups = load_firmgroups()
    expanded_groups = pd.Series()
    for fid in firms_joingroups.values.flat:
        if np.isnan(fid) or fid == -1:
            continue
        wut = (firms_joingroups == fid).max(axis=1)
        assert wut.sum() == 1   # Firm only matched to one group
        group_rep = wut[wut].index.values[0]
        group = firms_joingroups.loc[group_rep, :].dropna().tolist()
        group = [int(x) for x in group if x != -1]
        expanded_groups = expanded_groups.append(pd.Series({fid: group}))

    # Add `facid`s that never needed eyeballing
    for fid in unique_facids:
        if fid not in expanded_groups:
            expanded_groups.loc[fid] = [fid]

    return expanded_groups


@load_or_build(data_path('pr2_joinfirms.p'))
def load_firmgroups():

    geocodes = load_pr2geocodes()
    combos = combine_on_location(geocodes)
    return combos


def combine_on_location(geocodes):
    emi = build_int_qcer().set_index(['facid', 'year', 'mth']).sort_index()
    emi = emi['emi']

    # Get 'equiv_classes' by distance
    dists = getdist(geocodes, geocodes, within=.4)
    dists.index = dists.columns
    firm_groups = equiv_class(dists)
    # Eyeball groups with more than one member
    firm_groups = firm_groups[firm_groups[1].notnull()]
    firm_groups_list = [b.dropna().astype(int).tolist()
                        for a, b in firm_groups.iterrows()]
    actor = FirmInteract(firm_groups_list, geocodes=geocodes, emi=emi)
    classes = actor.interact(filepath='../data/geocode/firmgroups.csv')

    return classes


class FirmInteract(DataInteractModel):

    def __init__(self, *args, **kwargs):
        super(FirmInteract, self).__init__(*args, **kwargs)
        self.facid_names = read_pr2table('rtc_fac_names').set_index('facid')
        self.prompt_str = ('\nCombine [a]ll, [s]ome, [n]one ([f]lag);'
                           'or [m]ap, [p]lay w/ data >> ')
        # Also, 'tb' shortcut for (combine all, notes: textbook)
        good_input = ('a', 's', 'n', 'e', 'p', 'm', 'tb')
        self.good_input = good_input + tuple(['f' + x for x in good_input])

        self.manual_groups = self.set_groups_manually()

        self.drawmap = False
        self._istextbook = False

    def set_groups_manually(self):
        """
        Key is min(group). '-1' is a flag for 'may have duplicate entries' or
        some other oddity.
        """
        manual = {
            127381:	([127381, 132191], 'One has no address, but def match'),
            127380:	([127380, 132192], 'One has no address, but def match'),
            55239:	([55239, 117227], ''),
            115211:	([115211], ''),
            12224:	([12224], ''),
            131003:	([131003, 800012], ''),
            16299:	([16299, 101977], ''),
            16274:	([16274, 132626], ''),
            800264:	([800264], ''),
            3968:	([3968], ''),
            94079:	([94079, 114736, 118406], ''),
            6012:	([6012, 103672, 115449], ''),
            22373:	([22373], ''),
            99588:	([99588], ''),
            115277:	([115277], ''),
            53080:	([53080], ''),
            800037:	([800037], ''),
            10057:	([10057], ''),
            17418:	([119920, 121240, 17418, -1], 'tb, dups in 1999 m3 m6 m12'),
            55711:	([55711], ''),
            60531:	([60531], ''),
            800099:	([800099], ''),
            7427:	([7427], ''),
            10055:	([129497, 10055, -1],
                    'Maybe overlap, firms right on top of each other'),
            68118:	([68118], ''),
            18235:	([18235, 104013], ''),
            38440:	([38440], ''),
            1836:	([1836, 109198, 136516], ''),
            83753:	([83753, 133987, -1], 'tb, dupe 2004m9 ?'),
            7931:	([7931], 'Look like 75373, isnt per pr3 emit data'),
            75373:	([75373, 112853], ''),
            800295:	([800295], ''),
            112365:	([112365, 122295, -1], 'tb, dup 2000m3'),
            14052:	([14052, 115536, -1], 'tb, dup 1998m6'),
            57035:	([57035, 108805, 114264], ''),
            5830:	([5830], ''),
            17400:	([17400], ''),
            75411:	([75411, 133046, -1],
                    'tb, dup in 2003m6 ? (m3 is missing...)'),
            94872:	([94872], ''),
            5768:	([5768], ''),
            56427:	([56427], ''),
            104017:	([104017, 800241, -1], 'tb, dup 1995m3'),
            800319:	([800319, 800362], ''),
            75479:	([75479], ''),
            17953:	([17953], ''),
            22047:	([22047, 138568, -1], 'tb, duplication in 2003m12 ?'),
            87571:	([87571, 109229, 117572, 134781, 144792], ''),
            11790:	([11790, 122822], ''),
            44551:	([44551, 124838, -1], 'tb, dup 2001m12'),
            50098:	([50098], ''),
            800016:	([800016], ''),
            800259:	([800431, 800259], ''),
            15872:	([15872, 115666], ''),
            800223:	([800223, 800370, -1], 'tb, dup 1998 m12'),
            800219:	([800219, 800409], ''),
            114138:	([114138, 139010, 800204, 117151], ''),
            101843:	([101843, 126351, 141585], ''),
            17840:	([17840, 137471], ''),
            800150:	([800150, 800344], ''),
            800144:	([800144, 800363], ''),
            115314:	([800124, 800420, 115314], ''),
            115394:	([115394, 800125, 800416, -1], 'tb, dup 2003 m 12'),
            800123:	([800123, 800417], ''),
            800103:	([800373, 800103], ''),
            42333:	([42333, 106797], ''),
            21837:	([21837, 117140], ''),
            73022:	([73022], ''),
            800192:	([800192, 800391], ''),
            126501:	([126501, 147754], ''),
            46268:	([46268, 73899, -1], 'prob overlap but weird'),
            12185:	([12185], ''),
            11435:	([11435], ''),
            130211:	([800208, 130211, -1], 'tb, maybe dup in 2002 m9'),
            9053:	([9053, 137977], ''),
            25638:	([25638], 'Per pr3 emit data, not linked'),
            128243:	([128243], 'Per pr3 emit data, not linked'),
            117785:	([117785, 800109], ''),
            15381:	([15381, 112164, 133996, -1], 'tb, dup 2004m9'),
            7179:	([7179, 107656], ''),
            9729:	([123774, 9729, -1], 'tb, dups 2000 m3 (maybe) m12 (def)'),
            1026:	([1026, 115778], ''),
            23542:	([23542, 119104], ''),
            18763:	([18763, 115663, 137520], ''),
            11197:	([11197, 121746, 140499], ''),
            60342:	([60342, 147764], ''),
            54183:	([54183, 102299, -1], 'all duplicates'),
            50079:	([50079, 105318, 110720], ''),
            54167:	([54167, 110671, 114457], ''),
            21395:	([21395, 107654], ''),
            93073:	([93073, 124723, -1], 'tb, dup 2003m12'),
            22164:	([22164, 107655], ''),
            7053:	([7053, 133405], ''),
            63180:	([63180], ''),
            11103:	([11103, 109879, 132068], ''),
            861:	([861, 124619], ''),
            13136:	([13136, 104015], ''),
            117581:	([117581, 134768, 144791], ''),
            62281:	([62281, 142267], ''),
            14472:	([14472, 126498], ''),
            82727:	([82727, 145188], ''),
            800218:	([800218, 800408], ''),
            19212:	([19212], ''),
            22364:	([22364], ''),
            2825:	([2825], ''),
            57329:	([57329], ''),
            11016:	([11016], ''),
            132071:	([132071], ''),
            51935:	([51935, 107659], ''),
            800110:	([800110], ''),
            129729:	([129729], ''),
            115389:	([800126, 800419, 115389, -1],
                     'tb, maybe dups in 1998m6, 2003m12'),
            133813:	([143261, 133813], ''),
            10915:	([10915, 107657], ''),
            800070:	([800070, 800393], ''),
            127648:	([127648], ''),
            800170:	([800170], ''),
            42577:	([42577, 113240], ''),
            23196:	([23196], ''),
            109208:	([800342, 109208, -1], 'serial dups 1997-98'),
            131732:	([800210, 131732], ''),
            115315:	([115315, 800224], ''),
            59968:	([59968, 110982], ''),
            115241:	([115241], ''),
            125579:	([125579], ''),
            57892:	([57892, 144455], ''),
            90307:	([90307], ''),
            14855:	([14855, 141012, -1], 'tb, dup 2004m12'),
            15544:	([15544], ''),
            57818:	([57818], ''),
            20564:	([20564], ''),
            31046:	([31046], ''),
            37365:	([37365, 106810], ''),
            115172:	([800343, 115172], ''),
            45527:	([45527], ''),
            60540:	([60540], ''),
            61210:	([61210], ''),
            55758:	([55758], ''),
            25016:	([25016, 141555], ''),
            65974:	([65974, 119907], ''),
            55714:	([55714], ''),
            18865:	([18865], ''),
            20899:	([20899, 126050, 137508, -1], 'tb, dup 2001m3'),
            117485:	([117485, 800153], ''),
            2443:	([2443, 119134], ''),
            57722:	([57722, 108701], ''),
            20797:	([20797, 107653], ''),
            4451:	([4451], ''),
            115041:	([115041], ''),
            115040:	([115040], ''),
            115002:	([115002], ''),
            69677:	([69677, 123087, -1], 'tb, duplication 2000m6 ?'),
            117006:	([117006, 800115, 800372, -1], 'tb, dup in 1998m12'),
            7854:	([7854, 101499], ''),
            6394:	([6394], ''),
            51438:	([51438, 103618], ''),
            117247:	([117247, 800222, -1], 'tb, dup 1998m12'),
            3417:	([3417], ''),
            129238:	([800310, 129238], ''),
            59547:	([59547, 113415], ''),
            63626:	([63626, 108113, 800196], ''),
            47232:	([47232, 131249, -1],
                    'looks like overlap, but very fuzzy trans'),
            106325:	([106325], ''),
            6505:	([6505, 118618, -1], 'tb, dups 1999m3-6'),
            114801:	([114801, 800131], ''),
            125015:	([125015, 800213], ''),
            22607:	([22607], ''),
            50813:	([50813], ''),
            75:	([75, 102969, 131850], ''),
            66226:	([66226, 113160], ''),
            83444:	([83444, 117290], ''),
            18455:	([18455], ''),
        }
        return manual

    def display(self, row):
        id_list = row
        id_list.sort_values()

        rep = min(id_list)
        # I've already done this group, set manually in `set_groups_manually`
        if rep in self.manual_groups:
            keep_idlist, notes = self.manual_groups[rep]
            pass_idlist = [x for x in id_list if x not in keep_idlist]

        # It's a singleton, no matching possible
        elif len(row) == 1:
            keep_idlist = row
            pass_idlist = None
            notes = ''

        # Actual matching
        else:
            header_str, new_order = self._display_str_header(id_list)
            emi_str = self._display_str_emi(new_order)
            full_prompt = ''.join([emi_str, header_str, self.prompt_str])

            reply = self._force_valid_response(full_prompt, self.good_input)
            keep_idlist, pass_idlist = self.parse_reply(reply, new_order)
            notes = None

        # If there are leftovers, put them next in the queue
        if pass_idlist:
            self.looplist.append(pass_idlist)

        output = self.format_output(keep_idlist, notes=notes)

        return output

    def _display_str_header(self, firm_list):
        """Create header (firm meta info) string"""

        firmgroup = self.geocodes.loc[firm_list, :].copy()
        # Merge in names
        firmgroup = firmgroup.join(self.facid_names)
        # Make new 'row' index sorted west to east
        firmgroup = firmgroup.sort_values(
            'utm_east').reset_index().reset_index()
        new_order = firmgroup['facid'].tolist()
        # Calc centroid, relative utm
        centroid = firmgroup[UTM].mean().astype(int)
        firmgroup[UTM] -= centroid

        # Make ID numbers strings for fixed width printing
        # Characters: Row (2) facid (6)
        firmgroup['row'] = firmgroup['index'].astype(str).str.ljust(2)
        firmgroup['facid'] = firmgroup['facid'].astype(str).str.ljust(6)
        firmgroup['match_address'] = firmgroup['match_address'].str.replace(', USA', '')  #noqa

        headers_header = '\nChoice, facid\n'
        firm_header = ('{row} {facid} {utm_east}, {utm_north}\n'
                       '   {name}\n'
                       '   {address}\n'
                       '   {match_address}\n'
                       '-------------------------\n')

        header_str = headers_header
        for _, row in firmgroup.iterrows():
            header_str += firm_header.format(**row.to_dict())

        # Show points on google maps
        if self.drawmap:
            draw_googlemap(firmgroup['x'].values, firmgroup['y'].values,
                           filepath='map_{}'.format(firmgroup['facid'].min()))
            self.drawmap = False

        return header_str, new_order

    def _display_str_emi(self, firm_list):
        groups_emi = self.emi.loc[pd.IndexSlice[firm_list, :, :]]
        wide = groups_emi.unstack(level=0)
        # Sort the firms in the order of `firm_list` and make a string
        wide_str = wide[firm_list].to_string()
        wide_str = '\n=====================\n' + wide_str
        return wide_str

    def parse_reply(self, reply, id_list):

        row_list = range(len(id_list))

        flag = 'f' in reply
        reply = reply.replace('f', '')

        if reply == 'tb':
            keep_rows, pass_rows = row_list, None
            self._istextbook = True and not flag  # If flag, still want notes
        elif reply == 'a':
            keep_rows, pass_rows = row_list, None
        elif reply == 's':
            keep_rows = self._force_valid_response('Which firms? >> ', row_list,
                                                   listin=True, dtype=int)
            pass_rows = [x for x in row_list if x not in keep_rows]
        elif reply == 'n':
            keep_rows, pass_rows = None, None
        elif reply == 'e':
            self.looplist = []
            keep_rows, pass_rows = None, None
        elif reply == 'p':
            keep_rows, pass_rows = None, row_list
            import ipdb
            ipdb.set_trace()
        elif reply == 'm':
            keep_rows, pass_rows = None, row_list
            self.drawmap = True
        else:
            raise ValueError("The humanity")

        keep_ids = self._rowidx_to_id(keep_rows, id_list)
        pass_ids = self._rowidx_to_id(pass_rows, id_list)

        if flag:
            keep_ids.append(-1)

        return keep_ids, pass_ids

    def _rowidx_to_id(self, rowlist, idlist):
        if rowlist is None:
            return None
        else:
            return [x for idx, x in enumerate(idlist) if idx in rowlist]

    def format_output(self, keep_list, notes=None):

        if keep_list is None:
            return None, None

        group_rep = min([x for x in keep_list if x > 0])

        equiv_class = pd.Series(keep_list, name=group_rep)

        if self._istextbook:
            notes = 'tb'
            self._istextbook = False
        elif notes is None:
            notes = raw_input('Notes on choice >>> ')

        notes_series = pd.Series(notes, name=group_rep)

        return equiv_class, notes_series


if __name__ == '__main__':
    df = load_firmgroups(_load=False)
