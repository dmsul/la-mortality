import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

from econtools import load_or_build, DataInteractModel, force_list

from util.system import data_path
from util.gis import clean_street
from clean.nei import load_nei1999_table, load_nei
from clean.pr2.rawio import read_pr2table, build_int_qcer
from clean.pr2.firmgroups import group_lists_full


@load_or_build(data_path('pr2_nei_id.p'))
def get_nei_id():

    pr2 = load_pr2data()
    onid = direct_stateid_match(pr2)
    add_matches = address_match(pr2, onid)
    # Combine direct and address matches
    nei_ids = pd.Series()
    for facid in pr2.index.unique():
        direct = force_list(onid.get(facid, []))
        both = add_matches.loc[facid, 'state_id'] + direct
        nei_ids.loc[facid] = both

    # See if every firm has one (at least in location group)
    firms_joingroups = group_lists_full()
    has_id = pd.Series()
    for facid, sids in nei_ids.iteritems():
        has_one = len(sids) > 0
        if not has_one:
            group_facids = firms_joingroups.loc[facid]
            for gfacid in group_facids:
                if len(nei_ids.loc[gfacid]) > 0:
                    has_one = True
                    break

        has_id.loc[facid] = has_one

    # Make sure the guys without NEI state_id's are small, end before1999, etc.
    # [3/6/15] It's good.
    if 1 == 0:
        emi = build_int_qcer().set_index('facid')[['year', 'mth', 'emi']]
        for facid in has_id[~has_id].index:
            this_emi = emi.loc[facid, :]
            print this_emi
            import ipdb
            ipdb.set_trace()

    nei_ids = nei_ids[has_id]

    return nei_ids

def load_pr2data():     #noqa
    adds = read_pr2table('rtc_address', clean=True)
    names = read_pr2table('rtc_fac_names').set_index('facid')
    firm_info = adds.join(names)
    assert firm_info['street'].notnull().min()

    firm_info.reset_index(inplace=True)
    firm_info['state_id_model'] = firm_info.apply(_make_nei_id, axis=1)
    firm_info.set_index('facid', inplace=True)

    return firm_info

def _make_nei_id(x):    #noqa

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
    # RECLAIM firms are all (AB, DIS) = ('SC', 'SC')

    # pr2 data don't have any county data. Instead, just loop through all
    # possible values, which should be redundant after all other SC-specific
    # info
    co = '{}'

    abcode = str(ABCODE_XWALK['SC']).rjust(2, '0')
    discode = str(DISCODE_XWALK['SC']).rjust(2, '0')
    facid = str(int(x['facid']))
    nei_id = co + abcode + discode + facid
    return nei_id


def direct_stateid_match(pr2):
    all_ids = _all_state_ids()
    # No county codes in PR2 data, so try them all.
    by_co = []
    for co in (19, 30, 33, 36):
        temp_state_id = pr2['state_id_model'].apply(lambda x: x.format(co))
        temp_state_id.name = co
        has_co = temp_state_id.apply(lambda x: x in all_ids)
        by_co.append(has_co)
    matches = pd.concat(by_co, axis=1)
    # Make sure only one county code matches
    assert matches.sum(axis=1).max() <= 1
    # Fill in correct county code
    co_code = matches[matches.max(axis=1)].idxmax(axis=1)
    co_code.name = 'co'
    xwalk_stateid = pr2.join(co_code, how='inner')
    xwalk_stateid = xwalk_stateid.apply(
        lambda x: x['state_id_model'].format(x['co']), axis=1)
    xwalk_stateid.name = 'state_id'

    return xwalk_stateid

def _all_state_ids():  #noqa
    id_sets = []
    id_sets.append(load_nei1999_table('si')[['state_id']].drop_duplicates())
    id_sets.append(load_nei(2002)[['state_id']].drop_duplicates())
    id_sets.append(load_nei(2005)[['state_id']].drop_duplicates())
    all_ids = pd.concat(id_sets).drop_duplicates().squeeze()
    return tuple(all_ids)


def address_match(pr2, xwalk_stateid):
    pr2['street'] = pr2['street'].apply(clean_street)
    pr2['name'] = std_firm_name(pr2['name'])
    nei_adds = load_nei_adds()
    firms_joingroups = group_lists_full()
    unique_facids = pr2.index.unique().tolist()

    add_match_matrix = _address_match_matrix(pr2, nei_adds)

    # Eyeball address matches
    wut = AddMatch(unique_facids,
                   add_ratios=add_match_matrix,
                   pr2=pr2,
                   xwalk_stateid=xwalk_stateid,
                   nei_adds=nei_adds,
                   joingroups=firms_joingroups)
    df = wut.interact('../data/geocode/sid_add_match.csv')

    # Check for duplications [3/6/15, only offshore and del valle]
    wut.check_dups(df)

    return df

def load_nei_adds():   #noqa
    keep_vars = ['state_id', 'street', 'city', 'name']

    nei1999 = load_nei1999_table('si')[keep_vars].drop_duplicates()
    nei2002 = load_nei(2002)[keep_vars].drop_duplicates()
    nei2005 = load_nei(2005)[keep_vars].drop_duplicates()
    nei_adds = pd.concat([nei1999, nei2002, nei2005])

    # Standardize names (periods, commas, then spaces)
    nei_adds['name'] = std_firm_name(nei_adds['name'])
    # Standardize 'street'
    nei_adds['street'] = nei_adds['street'].apply(clean_street)
    # Drop PO Boxes
    nei_adds = nei_adds[~nei_adds['street'].str.contains('^PO BOX')]
    # Correct obvious address typos
    _fix_st_typos(nei_adds, 'WANAHEIM', 'W ANAHEIM')
    # Drop duplicates again
    nei_adds = nei_adds.drop_duplicates()
    nei_adds = nei_adds.drop_duplicates(['state_id', 'street'])

    nei_adds.reset_index(drop=True, inplace=True)
    return nei_adds

def _fix_st_typos(df, patt, rep):   #noqa
    df['street'] = df['street'].replace(patt, rep, regex=True)

def _address_match_matrix(pr2, nei_adds):   #noqa
    df = pd.DataFrame(np.zeros((pr2.shape[0], nei_adds.shape[0]), dtype=int),
                      index=pr2.index, columns=nei_adds.index)
    for idx, add in pr2['street'].iteritems():
        ratios = nei_adds['street'].apply(lambda x: fuzz.ratio(x, add))
        df.loc[idx, :] = ratios

    return df


def std_firm_name(s):
    s = s.replace('\.', ' ', regex=True)
    s = s.replace(',', ' ', regex=True)
    s = s.replace('\s\s+', ' ', regex=True)
    s = s.str.strip()
    s = s.str.upper()
    return s


class AddMatch(DataInteractModel):

    def __init__(self, *args, **kwargs):
        super(AddMatch, self).__init__(*args, **kwargs)

        self.addmatch_min = 90
        self.matches = []

        self.prompt_str = ('Use [a]ll, [s]ome, [n]one '
                           '([f]lag, [t]ake notes) >>> ')
        good_input = ('a', 's', 'n', 'e', 'd')
        flags = ('t', 'f', '')
        self.good_input = tuple([a + b for a in flags for b in good_input])

        self.manual_groups = self.set_groups_manually()

    def set_groups_manually(self):
        manual = {
            800431:	([], False, ''),
            800420:	([], False, ''),
            800419:	([], False, ''),
            800417:	([], False, ''),
            800416:	([], False, ''),
            800409:	([], False, ''),
            800408:	([], False, ''),
            800393:	([], False, ''),
            # 800391:	(['191026800196'], False, ''),
            800391:	([], False, ''),
            800373:	([], False, ''),
            800372:	([], False, ''),
            800371:	([], False, ''),
            800370:	(['T$12697'], False, ''),
            800363:	(['191026111642', 'T$12705'], False, ''),
            800362:	(['T$12700'], False, ''),
            800344:	([], False, ''),
            800343:	([], False, ''),
            800342:	([], False, ''),
            800339:	([], False, ''),
            800338:	([], False, ''),
            800337:	([], False, ''),
            800335:	([], False, ''),
            800330:	([], False, ''),
            800329:	([], False, ''),
            800326:	([], False, ''),
            800325:	([], False, ''),
            800319:	(['T$12700'], False, ''),
            800310:	([], False, ''),
            800295:	([], False, ''),
            800273:	([], False, ''),
            800264:	([], False, ''),
            800263:	([], False, ''),
            800259:	([], False, ''),
            800258:	([], False, ''),
            800241:	([], False, ''),
            800240:	([], False, ''),
            800232:	([], False, ''),
            800224:	(['EGU0139'], False, ''),
            800223:	(['T$12697'], False, ''),
            800222:	(['T$12726', '90810TXCRF23208'], False, ''),
            800219:	([], False, ''),
            800218:	([], False, ''),
            800213:	([], False, ''),
            800210:	([], False, ''),
            800208:	([], False, ''),
            800205:	([], False, ''),
            800204:	([], False, ''),
            800196:	([], False, ''),
            800193:	(['EGU0112'], False, ''),
            800192:	([], False, ''),
            800189:	([], False, ''),
            800184:	([], False, ''),
            800183:	(['19102675290'], False, ''),
            800182:	([], False, ''),
            800181:	([], False, ''),
            800170:	([], False, ''),
            800168:	([], False, ''),
            800167:	([], False, ''),
            800154:	([], False, ''),
            800153:	([], False, ''),
            800150:	([], False, ''),
            800149:	([], False, ''),
            800144:	(['191026111642', 'T$12705'], False, ''),
            800131:	([], False, ''),
            800128:	([], False, ''),
            800127:	([], False, ''),
            800126:	([], False, ''),
            800125:	([], False, ''),
            800124:	([], False, ''),
            800123:	([], False, ''),
            800115:	([], False, ''),
            800113:	([], False, ''),
            800111:	([], False, ''),
            800110:	([], False, ''),
            800109:	([], False, ''),
            800103:	([], False, ''),
            800099:	([], False, ''),
            800094:	([], False, ''),
            800089:	(['T$12648'], False, ''),
            800088:	([], False, ''),
            800080:	([], False, ''),
            800078:	([], False, ''),
            800075:	(['EGU0111'], False, ''),
            800074:	([], False, ''),
            800070:	([], False, ''),
            800069:	([], False, ''),
            800067:	([], False, ''),
            800066:	([], False, ''),
            800047:	([], False, ''),
            800039:	([], False, ''),
            800038:	([], False, ''),
            800037:	([], False, ''),
            800030:	(['T$12602'], False, ''),
            800026:	(['T$12706'], False, ''),
            800016:	([], False, ''),
            800012:	(['191026139378', '19102638655'], False, ''),
            800003:	([], False, ''),
            147764:	([], False, ''),
            147754:	([], False, ''),
            145188:	(['T$18376'], False, ''),
            144792:	([], False, ''),
            144791:	(['301026109199'], False, ''),
            144455:	([], False, ''),
            143741:	(['301026108616'], False, ''),
            143740:	([], False, ''),
            143739:	([], False, ''),
            143261:	([], False, ''),
            142536:	([], False, ''),
            142267:	([], False, ''),
            142189:	([], False, ''),
            142187:	(['301026108616'], False, ''),
            141585:	([], False, ''),
            141555:	([], False, ''),
            141012:	(['T$18347'], False, ''),
            140499:	([], False, ''),
            139796:	([], False, ''),
            139010:	([], False, ''),
            138568:	([], False, ''),
            137977:	([], False, ''),
            137520:	(['EGU0107'], False, ''),
            137508:	([], False, ''),
            137471:	([], False, ''),
            136516:	([], False, ''),
            135978:	([], False, ''),
            135976:	(['301026109207'], False, ''),
            135974:	(['301026108616'], False, ''),
            134781:	([], False, ''),
            134768:	(['301026109199'], False, ''),
            133996:	([], False, ''),
            133987:	([], False, ''),
            133813:	([], False, ''),
            133405:	([], False, ''),
            133046:	([], False, ''),
            132626:	(['T$18308'], False, ''),
            132192:	([], False, ''),
            132191:	([], False, ''),
            132071:	([], False, ''),
            132068:	([], False, ''),
            131850:	([], False, ''),
            131824:	([], False, ''),
            131732:	(['T$18445'], False, ''),
            131249:	(['T$18325'], False, ''),
            131003:	(['90749RCPRD1801E', 'T$12709'], False, ''),
            130211:	([], False, ''),
            129816:	([], False, ''),
            129810:	([], False, ''),
            129729:	([], False, ''),
            129497:	([], False, ''),
            129238:	([], False, ''),
            128243:	([], False, ''),
            127648:	([], False, ''),
            127299:	(['331526127299'], False, ''),
            126501:	([], False, ''),
            126498:	([], False, ''),
            126351:	([], False, ''),
            126050:	([], False, ''),
            125579:	([], False, ''),
            125015:	([], False, ''),
            124838:	([], False, ''),
            124808:	([], False, ''),
            124723:	([], False, ''),
            124619:	([], False, ''),
            123774:	(['T$12670'], False, ''),
            123087:	([], False, ''),
            122822:	([], False, ''),
            122295:	([], False, ''),
            122012:	(['191026108763', '191026109206'], False, ''),
            121746:	([], False, ''),
            121737:	([], False, ''),
            121240:	([], False, ''),
            119920:	([], False, ''),
            119907:	([], False, ''),
            119134:	([], False, ''),
            119104:	([], False, ''),
            118618:	([], False, ''),
            118406:	([], False, ''),
            117785:	([], False, ''),
            117581:	(['301026109199'], False, ''),
            117572:	([], False, ''),
            117485:	([], False, ''),
            117290:	([], False, ''),
            117247:	(['T$12726', '90810TXCRF23208'], False, ''),
            117227:	([], False, ''),
            117151:	([], False, ''),
            117140:	([], False, ''),
            117006:	([], False, ''),
            115778:	(['361026121737'], False, ''),
            115666:	([], False, ''),
            115663:	(['EGU0107'], False, ''),
            115563:	([], False, ''),
            115536:	(['EGU0108'], False, ''),
            115449:	([], False, ''),
            115394:	(['EGU0106', '191026800125'], False, ''),
            115389:	(['EGU0121'], False, ''),
            115315:	(['EGU0139'], False, ''),
            115314:	([], False, ''),
            115277:	([], False, ''),
            115241:	([], False, ''),
            115211:	([], False, ''),
            115172:	([], False, ''),
            115130:	([], False, ''),
            115041:	([], False, ''),
            115040:	([], False, ''),
            115002:	([], False, ''),
            114997:	([], False, ''),
            114801:	([], False, ''),
            114736:	([], False, ''),
            114457:	([], False, ''),
            114264:	([], False, ''),
            114138:	([], False, ''),
            113415:	([], False, ''),
            113240:	([], False, ''),
            113160:	([], False, ''),
            112853:	([], False, ''),
            112365:	([], False, ''),
            112164:	([], False, ''),
            111415:	(['36102623215'], False, ''),
            110982:	([], False, ''),
            110720:	([], False, ''),
            # 110671:	(['19102620872'], False, ''),
            110671:	([], False, ''),
            109879:	([], False, ''),
            109229:	([], False, ''),
            109208:	([], False, ''),
            109207:	([], False, ''),
            109198:	([], False, ''),
            109192:	([], False, ''),
            108805:	([], False, ''),
            108763:	(['191026109206'], False, ''),
            108701:	([], False, ''),
            108616:	([], False, ''),
            108113:	([], False, ''),
            107659:	([], False, ''),
            107657:	([], False, ''),
            107656:	([], False, ''),
            107655:	([], False, ''),
            107654:	(['1910264528'], False, ''),
            107653:	([], False, ''),
            106810:	([], False, ''),
            106797:	([], False, ''),
            106325:	([], False, ''),
            105356:	([], False, ''),
            105318:	([], False, ''),
            105277:	([], False, ''),
            104571:	([], False, ''),
            104018:	([], False, ''),
            104017:	([], False, ''),
            104015:	([], False, ''),
            104013:	([], False, ''),
            104012:	([], False, ''),
            103672:	([], False, ''),
            103618:	([], False, ''),
            102969:	([], False, ''),
            102299:	([], False, ''),
            101977:	(['191026660'], False, ''),
            101843:	([], False, ''),
            101656:	(['T$12692'], False, ''),
            101578:	(['19102645086'], False, ''),
            101499:	([], False, ''),
            101369:	([], False, ''),
            101039:	([], False, ''),
            100844:	([], False, ''),
            100291:	([], False, ''),
            100130:	([], False, ''),
            99588:	([], False, ''),
            98949:	(['301026108616'], False, ''),
            98812:	([], False, ''),
            98159:	([], False, ''),
            97081:	([], False, ''),
            96587:	([], False, ''),
            95524:	([], False, ''),
            95212:	([], False, ''),
            94930:	([], False, ''),
            94872:	([], False, ''),
            94079:	([], False, ''),
            93346:	([], False, ''),
            93073:	([], False, ''),
            92019:	([], False, ''),
            90307:	([], False, ''),
            89429:	([], False, ''),
            89248:	([], False, ''),
            87571:	([], False, ''),
            85943:	([], False, ''),
            84223:	([], False, ''),
            83753:	([], False, ''),
            83738:	([], False, ''),
            83444:	([], False, ''),
            83278:	([], False, ''),
            83102:	([], False, ''),
            82727:	(['T$18376'], False, ''),
            82022:	([], False, ''),
            79397:	([], False, ''),
            75479:	([], False, ''),
            75411:	([], False, ''),
            75373:	([], False, ''),
            74424:	([], False, ''),
            73899:	(['T$12903'], False, ''),
            73790:	([], False, ''),
            73635:	([], False, ''),
            73022:	([], False, ''),
            72351:	([], False, ''),
            69690:	([], False, ''),
            69677:	([], False, ''),
            68122:	([], False, ''),
            68118:	([], False, ''),
            68117:	([], False, ''),
            68042:	([], False, ''),
            67945:	([], False, ''),
            66226:	([], False, ''),
            65974:	([], False, ''),
            65384:	(['T$12773'], False, ''),
            63626:	([], False, ''),
            63180:	([], False, ''),
            62897:	([], False, ''),
            62281:	([], False, ''),
            61970:	([], False, ''),
            61962:	([], False, ''),
            61722:	([], False, ''),
            61589:	([], False, ''),
            61210:	([], False, ''),
            61209:	([], False, ''),
            60540:	([], False, ''),
            60531:	([], False, ''),
            60342:	([], False, ''),
            59968:	([], False, ''),
            59618:	([], False, ''),
            59547:	([], False, ''),
            58622:	([], False, ''),
            57892:	([], False, ''),
            57818:	([], False, ''),
            57722:	([], False, ''),
            57329:	([], False, ''),
            57304:	(['191026106325'], False, ''),
            57035:	([], False, ''),
            56940:	([], False, ''),
            56427:	(['36102643436'], False, ''),
            55865:	([], False, ''),
            55758:	([], False, ''),
            55714:	([], False, ''),
            55711:	([], False, ''),
            55349:	([], False, ''),
            55239:	([], False, ''),
            55221:	([], False, ''),
            54723:	([], False, ''),
            54402:	([], False, ''),
            54183:	([], False, ''),
            # 54167:	(['19102620872'], False, ''), # Both CBS, but second sid
                                                      # noqa not needed
            54167:	([], False, ''),
            53729:	([], False, ''),
            53080:	(['191026115277'], False, ''),
            52517:	([], False, ''),
            51949:	([], False, ''),
            51935:	([], False, ''),
            51620:	([], False, ''),
            51438:	([], False, ''),
            50813:	([], False, ''),
            50098:	([], False, ''),
            50079:	([], False, ''),
            47781:	([], False, ''),
            47771:	([], False, ''),
            47232:	([], False, ''),
            46500:	([], False, ''),
            46268:	(['T$12903'], False, ''),
            45953:	([], False, ''),
            45746:	([], False, ''),
            45527:	([], False, ''),
            45471:	([], False, ''),
            44551:	([], False, ''),
            43436:	([], False, ''),
            43201:	([], False, ''),
            42775:	([], False, ''),
            42676:	([], False, ''),
            42630:	([], False, ''),
            42577:	([], False, ''),
            42333:	([], False, ''),
            41794:	([], False, ''),
            41582:	([], False, ''),
            40764:	([], False, ''),
            40483:	([], False, ''),
            40196:	([], False, ''),
            40102:	([], False, ''),
            40034:	([], False, ''),
            40030:	([], False, ''),
            38872:	([], False, ''),
            38440:	([], False, ''),
            37603:	([], False, ''),
            37365:	([], False, ''),
            36363:	([], False, ''),
            35302:	([], False, ''),
            34055:	([], False, ''),
            31046:	([], False, ''),
            25638:	([], False, ''),
            25058:	([], False, ''),
            25016:	([], False, ''),
            24887:	([], False, ''),
            24242:	([], False, ''),
            24199:	([], False, ''),
            23907:	([], False, ''),
            23752:	([], False, ''),
            23589:	([], False, ''),
            23542:	([], False, ''),
            23449:	([], False, ''),
            23196:	(['T$18371'], False, ''),
            22911:	([], False, ''),
            22808:	([], False, ''),
            22607:	([], False, ''),
            22603:	([], False, ''),
            22373:	([], False, ''),
            22364:	([], False, ''),
            22164:	([], False, ''),
            22047:	([], False, ''),
            21887:	([], False, ''),
            21837:	([], False, ''),
            21598:	([], False, ''),
            21395:	([], False, ''),
            21290:	([], False, ''),
            20899:	([], False, ''),
            20797:	([], False, ''),
            20604:	([], False, ''),
            20564:	([], False, ''),
            20543:	([], False, ''),
            20203:	([], False, ''),
            19989:	([], False, ''),
            19563:	([], False, ''),
            19390:	([], False, ''),
            19212:	([], False, ''),
            19167:	([], False, ''),
            18984:	([], False, ''),
            18931:	(['361026100547'], False, ''),
            18865:	([], False, ''),
            18763:	(['EGU0107'], False, ''),
            18695:	([], False, ''),
            18648:	([], False, ''),
            18455:	([], False, ''),
            18294:	([], False, ''),
            18235:	([], False, ''),
            17956:	([], False, ''),
            17953:	([], False, ''),
            17840:	([], False, ''),
            17763:	([], False, ''),
            17623:	([], False, ''),
            17418:	([], False, ''),
            17400:	([], False, ''),
            17344:	([], False, ''),
            16978:	([], False, ''),
            16737:	(['BSCP4'], False, ''),
            16642:	(['T$18341'], False, ''),
            16639:	([], False, ''),
            16575:	([], False, ''),
            16531:	([], False, ''),
            16395:	([], False, ''),
            10141:	([], False, ''),
            10094:	([], False, ''),
            10057:	([], False, ''),
            10055:	([], False, ''),
            9755:	([], False, ''),
            9729:	([], False, ''),
            9217:	([], False, ''),
            9141:	([], False, ''),
            9114:	([], False, ''),
            16338:	([], False, ''),
            16299:	(['191026660'], False, ''),
            16274:	(['T$18308'], False, ''),
            15982:	([], False, ''),
            15872:	([], False, ''),
            15794:	([], False, ''),
            15544:	([], False, ''),
            15504:	([], False, ''),
            15381:	([], False, ''),
            15173:	([], False, ''),
            15164:	(['BSCP63'], False, ''),
            14944:	([], False, ''),
            14926:	([], False, ''),
            14871:	([], False, ''),
            14855:	(['T$18347'], False, ''),
            14736:	([], False, ''),
            14502:	([], False, ''),
            14495:	([], False, ''),
            14472:	([], False, ''),
            14445:	([], False, ''),
            14229:	([], False, ''),
            14092:	([], False, ''),
            14052:	(['EGU0108'], False, ''),
            14049:	([], False, ''),
            13976:	([], False, ''),
            13179:	([], False, ''),
            13136:	([], False, ''),
            12912:	([], False, ''),
            12428:	([], False, ''),
            12395:	(['191026108763'], False, ''),
            12372:	(['BSCP107'], False, ''),
            12247:	([], False, ''),
            12224:	([], False, ''),
            12185:	([], False, ''),
            12155:	([], False, ''),
            11887:	([], False, ''),
            11790:	([], False, ''),
            11716:	([], False, ''),
            11674:	([], False, ''),
            11640:	([], False, ''),
            11435:	([], False, ''),
            11197:	([], False, ''),
            11142:	([], False, ''),
            11119:	([], False, ''),
            11103:	([], False, ''),
            11034:	([], False, ''),
            11016:	([], False, ''),
            10915:	([], False, ''),
            10873:	([], False, ''),
            10340:	([], False, ''),
            9053:	([], False, ''),
            8791:	([], False, ''),
            8694:	([], False, ''),
            8582:	([], False, ''),
            8547:	([], False, ''),
            8439:	([], False, ''),
            7940:	([], False, ''),
            7931:	([], False, ''),
            7854:	([], False, ''),
            7427:	(['90058WNSLL2923F', 'T$18283'], False, ''),
            7416:	([], False, ''),
            7411:	([], False, ''),
            7179:	([], False, ''),
            7120:	([], False, ''),
            7053:	([], False, ''),
            6714:	([], False, ''),
            6505:	([], False, ''),
            6394:	([], False, ''),
            6281:	([], False, ''),
            6012:	([], False, ''),
            5998:	([], False, ''),
            5973:	([], False, ''),
            5830:	([], False, ''),
            5814:	([], False, ''),
            5768:	(['361026119940'], False, ''),
            5268:	([], False, ''),
            5181:	([], False, ''),
            4477:	([], False, ''),
            4451:	([], False, ''),
            4242:	([], False, ''),
            3968:	([], False, ''),
            3950:	([], False, ''),
            3721:	([], False, ''),
            3704:	([], False, ''),
            3585:	([], False, ''),
            3417:	([], False, ''),
            3029:	([], False, ''),
            2946:	([], False, ''),
            2912:	([], False, ''),
            2825:	([], False, ''),
            2537:	([], False, ''),
            2443:	([], False, ''),
            2418:	([], False, ''),
            2083:	([], False, ''),
            1962:	([], False, ''),
            1836:	([], False, ''),
            1744:	([], False, ''),
            1634:	([], False, ''),
            1073:	([], False, ''),
            1026:	(['361026121737'], False, ''),
            861:	([], False, ''),
            550:	([], False, ''),
            502:	([], False, ''),
            346:	([], False, ''),
            136:	([], False, ''),
            75:	([], False, ''),
        }
        return manual

    def display(self, facid):

        if facid in self.manual_groups:
            keep_stateid, flag, notes = self.manual_groups[facid]
            output = self.format_output(facid, keep_stateid, flag, notes)
            return output, None

        row = self.pr2.loc[facid, :]

        # Fuzz-match street address
        facids_ratios = self.add_ratios.loc[facid, :]
        matched_idx = facids_ratios[facids_ratios >= self.addmatch_min].index
        matched_neis = self.nei_adds.loc[matched_idx, :]
        matched_neis['add_match'] = facids_ratios.loc[matched_idx]
        # Get group's direct id matches, don't consider those here
        firms_group = self.joingroups.loc[facid]
        try:
            groups_ids = self.xwalk_stateid.loc[firms_group]
        except KeyError:
            groups_ids = []
        matched_neis = matched_neis[~matched_neis['state_id'].isin(groups_ids)]
        # Get name matches
        matched_neis['name_match'] = matched_neis['name'].apply(
            lambda x: fuzz.ratio(row['name'], x))
        # If only one left, call it good
        unique_matched = len(matched_neis['state_id'].unique())
        has_namematch = matched_neis['name_match'].max() > self.addmatch_min
        # Use it!
        if unique_matched == 1 and has_namematch:
            matched_neis = matched_neis.iloc[0, :]
            keep_stateid = list(set(force_list(matched_neis['state_id'])))
            flag = False
            notes = ''
        # No matches
        elif unique_matched == 0:
            keep_stateid, flag, notes = [], False, ''
        # Eyeball
        else:
            l = self.pr2.index.tolist()
            idx = l.index(facid)
            print "*"*20 + "\nRow {} of {}\n".format(idx, len(l)) + "*"*20
            # Fuzz-match city
            matched_neis['city_match'] = matched_neis['city'].apply(
                lambda x: fuzz.ratio(row['city'], x))
            matched_neis = matched_neis.sort_values(
                ['city_match', 'add_match', 'name_match'], ascending=False)
            keep_stateid, flag, notes = self.get_reply(row, matched_neis)

        output = self.format_output(facid, keep_stateid, flag, notes)
        return output, None

    def get_reply(self, firmrow, matched_neis):
        # Prompt strings
        firm_str = ("\n" + 20*"=" +
                    "\n\t    {name}\n\t    {street}, {city}\n" +
                    20*"-" + "\n\n")
        match_str = ("{name_match}\t{row}   {name}\n"
                     "{add_match}\t    {street}, {city}\n\n")

        prompt_list = [firm_str.format(**firmrow.to_dict())]
        matched_neis = matched_neis.reset_index()

        for idx, mrow in matched_neis.iterrows():
            mrow_dict = mrow.to_dict()
            mrow_dict.update({'row': idx})
            prompt_list.append(match_str.format(**mrow_dict))

        full_prompt = ''.join(prompt_list) + self.prompt_str
        # The reply
        reply = self._force_valid_response(full_prompt, self.good_input)
        keep_rows, flag, notes = self.parse_reply(reply, matched_neis.shape[0])
        # Get the (unique) data
        keep_stateid = list(set(
            matched_neis.loc[keep_rows, 'state_id'].tolist()))

        return keep_stateid, flag, notes

    def parse_reply(self, reply, N):

        id_list = range(N)

        take_notes = 't' in reply
        reply = reply.replace('t', '')

        flag = 'f' in reply
        reply = reply.replace('f', '')

        if reply == 'a':
            keep_rows = id_list
        elif reply == 's':
            keep_rows = self._force_valid_response('Which rows? >> ', id_list,
                                                   listin=True, dtype=int)
        elif reply == 'n':
            keep_rows = []
        elif reply == 'e':
            keep_rows = []
            self.looplist = []
        elif reply == 'd':
            import ipdb
            ipdb.set_trace()  # XXX BREAKPOINT
        else:
            raise ValueError("The humanity")

        if take_notes:
            notes = raw_input('Notes on choice >>> ')
        else:
            notes = ''

        return keep_rows, flag, notes

    def format_output(self, facid, *args):
        s = pd.Series(dict(zip(['state_id', 'flag', 'notes'], args)),
                      name=facid)
        return s

    def write_log(self, log_path, outdf, *args):
        with open(log_path, 'w') as f:
            f.write('Columns: {}\n\n'.format(outdf.columns.values))
            for idx, row in outdf.iterrows():
                key_str = "{}:\t"
                item_str = "({}, {}, '{}'),\n"  # id_matches, flag, notes
                full_line = key_str + item_str
                # Fix float/int/nan crap
                matches, flag, notes = row[['state_id', 'flag', 'notes']].tolist()  #noqa
                f.write(full_line.format(idx, matches, flag, notes))

    # For checking doubly-assigned `state_id`s after the main matching
    def check_dups(self, add_matches):

        self.add_matches = add_matches.copy()
        directly_matched_sids = self.xwalk_stateid.values

        for facid, sid_list in add_matches['state_id'].iteritems():
            for sid in sid_list:
                if sid in directly_matched_sids:
                    group_facids = self.joingroups.loc[facid]
                    try:
                        group_sids = self.xwalk_stateid.loc[group_facids]
                    except KeyError:
                        group_sids = []
                    oth_direct = self.xwalk_stateid[self.xwalk_stateid == sid].index  #noqa
                    if sid in group_sids:
                        pass
                    else:
                        facids_from_adds = self._get_facids_by_sid(sid)
                        nongroup_facids = [x for x in facids_from_adds
                                           if x not in group_facids]
                        if len(nongroup_facids) > 0:
                            self._display_dup(group_facids, nongroup_facids,
                                              oth_direct)

    def _display_dup(self, group_facids, nongroup_facids, oth_direct):
        group = self.pr2.loc[group_facids, :]
        oth = self.pr2.loc[oth_direct, :]
        non_group = self.pr2.loc[nongroup_facids, :]
        print group
        print oth
        print non_group
        import ipdb
        ipdb.set_trace()  # XXX Jury-rigged display and approve

    def _get_facids_by_sid(self, sid):
        facid_list = []
        for facid, sid_list in self.add_matches['state_id'].iteritems():
            if sid in sid_list:
                facid_list.append(facid)
        return facid_list


if __name__ == '__main__':
    df = get_nei_id()
