from os import path

import pandas as pd

from nose import runmodule
from nose.plugins.attrib import attr
from nose.tools import assert_equal, assert_raises
from pandas.util.testing import assert_frame_equal

from util.system import data_path, test_path
from atmods.io import AIRQ_PATH, LOCAL_CHUNK_PATH
from atmods.io import (normed_firmexp_path, normed_firmexp_fname,
                       filepath_airqdata, parse_firm_info,
                       load_full_exposure, load_firm_normed_exp,
                       load_firm_exposure, sum_allfirms_exposure,
                       sum_allfirms_exposure_mp, parse_kernmodel)


class TestAirqPath(object):

    def test_name_house_aermod_20(self):
        expected = 'hA20'
        result = normed_firmexp_fname('house', 'aermod', 20)
        assert_equal(expected, result)

    def test_name_house_unif5_37_c39(self):
        expected = 'hU5f37c39'
        geo = 'house'
        model = 'unif5'
        facid = 37
        cid = (3, 9)
        result = normed_firmexp_fname(geo, model, facid, chunk_info=cid)
        assert_equal(expected, result)

    def test_name_house_aermod_37_c3A(self):
        expected = 'hA37c3A'
        geo = 'house'
        model = 'aermod'
        facid = 37
        cid = (3, 10)    # To test for higher base conversion
        result = normed_firmexp_fname(geo, model, facid, chunk_info=cid)
        assert_equal(expected, result)

    def test_name_block_triw2_1(self):
        expected = 'bW2f1'
        result = normed_firmexp_fname('block', 'triw2', 1)
        assert_equal(expected, result)

    def test_name_zip_tria2_347(self):
        expected = 'pT2f347'
        result = normed_firmexp_fname('patzip', 'tria2', 347)
        assert_equal(expected, result)

    def test_rawpath(self):
        expected = path.join(AIRQ_PATH, 'pT2f347.p')
        result = normed_firmexp_path('patzip', 'tria2', 347)
        assert_equal(expected, result)

    def test_rawpath_chunk(self):
        expected = path.join(LOCAL_CHUNK_PATH, 'hU5f37c39.p')
        result = normed_firmexp_path('house', 'unif5', 37, chunk_info=(3, 9))
        assert_equal(expected, result)

    def test_raw_bad_geounit(self):
        assert_raises(ValueError, normed_firmexp_path, 'ocean', 'unif2', 1)

    def test_raw_bad_model(self):
        assert_raises(ValueError, normed_firmexp_path, 'house', 'flat7', 1)

    def test_filename_houses_aermod_path(self):
        expected = data_path('houses_aermod.p')
        result = filepath_airqdata('house', 'aermod')
        assert_equal(expected, result)

    def test_filename_blocks_tria5_elec(self):
        expected = data_path('blocks_tria5_el1.p')
        result = filepath_airqdata('block', 'tria5', elec=1)
        assert_equal(expected, result)

    def test_filename_patzip_unif2_nonelec(self):
        expected = data_path('patzips_unif2_el0.p')
        result = filepath_airqdata('patzip', 'unif2', elec=0)
        assert_equal(expected, result)

    def test_filename_badelec(self):
        assert_raises(ValueError, filepath_airqdata, 'patzip', 'unif2',
                      elec=True)


class TestDataCreation(object):
    @classmethod
    def setup_class(cls):
        cls.hA_full = load_data('houses_aermod.p')
        cls.bA_raw_firm = load_data('bA800192.p')
        cls.hT5_scaled_firm = load_data('hT5f115389_scaled.p')
        cls.hA_partialsum = load_data('hA_partialsum.p')
        cls.firms_for_partialsum = [136, 346, 550, 1026]

    @attr('slow')
    def test_fullaermod(self):
        expected = self.hA_full
        result = load_full_exposure('house', 'aermod')
        assert_frame_equal(expected, result)

    def test_firm_normed_aermod(self):
        expected = self.bA_raw_firm
        result = load_firm_normed_exp('block', 'aermod', 800192)
        assert_frame_equal(expected, result)

    def test_firm_tria5(self):
        expected = self.hT5_scaled_firm
        result = load_firm_exposure('house', 'tria5', 115389)
        assert_frame_equal(expected, result)

    @attr('slow')
    def test_sum_allfirms_exposure_mp(self):
        firms = self.firms_for_partialsum
        expected = self.hA_partialsum
        result = sum_allfirms_exposure_mp('house', 'aermod', firms)
        assert_frame_equal(expected.sort_index(), result.sort_index())

    @attr('slow')
    def test_sum_allfirms_data_partial(self):
        firms = self.firms_for_partialsum
        expected = self.hA_partialsum
        result = sum_allfirms_exposure('house', 'aermod', firm_list=firms)
        assert_frame_equal(expected, result)


class TestParsers(object):

    def test_chunk_aermod(self):
        expected = (1, 2, 3)
        filename = normed_firmexp_path('house', 'aermod',
                                       expected[0], expected[1:])
        result = parse_firm_info(filename)
        assert_equal(expected, result)

    def test_chunk_kernel(self):
        expected = (327, 1, 1)
        filename = normed_firmexp_path('block', 'unif5',
                                       expected[0], expected[1:])
        result = parse_firm_info(filename)
        assert_equal(expected, result)

    def test_kernname_unif4(self):
        expected = ('unif', 4)
        result = parse_kernmodel('unif4')
        assert_equal(expected, result)

    def test_kernname_badkern(self):
        name = 'wut7'
        assert_raises(ValueError, parse_kernmodel, name)

    def test_kernname_badband(self):
        name = 'unifh'
        assert_raises(ValueError, parse_kernmodel, name)


def load_data(filename):
    return pd.read_pickle(test_path(filename))


if __name__ == '__main__':
    import sys
    argv = [__file__, '-vs', '-a', '!slow'] + sys.argv[1:]
    runmodule(argv=argv, exit=False)
