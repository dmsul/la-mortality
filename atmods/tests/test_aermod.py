import nose
from pandas.util.testing import assert_frame_equal

import numpy as np
import pandas as pd

from atmods.aermod import Aermod


class TestAermod(object):

    def setUp(self):
        source = pd.DataFrame(
            {'facid': 1,
             'utm_east': 378647,
             'utm_north': 3782677,
             'type_shareF': .448,
             'pop1990': 8863164,
             'metsite_code': 'burk',
             'metsite_z': 175,
             'metsite_year': 9,
             'metsite_endyear': 9,
             'emit_share': 1 - .448,
             'stack_ht': 33.528,
             'stack_diam': 2.4384,
             'stack_veloc': 12.90829,
             'stack_temp': 413.3352}, index=[1])
        receptor = np.array(
            [source.utm_east[1] + 1000, source.utm_north[1] + 1000])

        self.receptor = pd.DataFrame(
            receptor.reshape(-1, 2), columns=['utm_east', 'utm_north'])
        self.receptors_2 = self.receptor.append(self.receptor.iloc[0, :] - 2000)

        self.source = source

        # Aermod output
        df1, df2 = self.init_aermod_results()
        self.expected_single = df1
        self.expected_double = df2

    def init_aermod_results(self):
        cols = ['utm_east', 'utm_north', 'exposure', 'month']
        df1 = pd.DataFrame(
            [
                [379647.0, 3783677.0, 0.01643396, 1],
                [379647.0, 3783677.0, 0.02145677, 2],
                [379647.0, 3783677.0, 0.02406834, 3],
                [379647.0, 3783677.0, 0.03086416, 4],
                [379647.0, 3783677.0, 0.01214121, 5],
                [379647.0, 3783677.0, 0.01989753, 6],
                [379647.0, 3783677.0, 0.01025806, 7],
                [379647.0, 3783677.0, 0.02305667, 8],
                [379647.0, 3783677.0, 0.01931035, 9],
                [379647.0, 3783677.0, 0.02198202, 10],
                [379647.0, 3783677.0, 0.02254476, 11],
                [379647.0, 3783677.0, 0.02013688, 12],
            ],
            columns=cols
        )
        df2 = pd.DataFrame(
            [
                [379647.0, 3783677.0, 0.01643396, 1],
                [377647.0, 3781677.0, 0.00847337, 1],
                [379647.0, 3783677.0, 0.02145677, 2],
                [377647.0, 3781677.0, 0.00499748, 2],
                [379647.0, 3783677.0, 0.02406834, 3],
                [377647.0, 3781677.0, 0.00454146, 3],
                [379647.0, 3783677.0, 0.03086416, 4],
                [377647.0, 3781677.0, 0.00386942, 4],
                [379647.0, 3783677.0, 0.01214121, 5],
                [377647.0, 3781677.0, 0.0034069, 5],
                [379647.0, 3783677.0, 0.01989753, 6],
                [377647.0, 3781677.0, 0.00385886, 6],
                [379647.0, 3783677.0, 0.01025806, 7],
                [377647.0, 3781677.0, 0.00408896, 7],
                [379647.0, 3783677.0, 0.02305667, 8],
                [377647.0, 3781677.0, 0.00453328, 8],
                [379647.0, 3783677.0, 0.01931035, 9],
                [377647.0, 3781677.0, 0.00403385, 9],
                [379647.0, 3783677.0, 0.02198202, 10],
                [377647.0, 3781677.0, 0.00407482, 10],
                [379647.0, 3783677.0, 0.02254476, 11],
                [377647.0, 3781677.0, 0.00694324, 11],
                [379647.0, 3783677.0, 0.02013688, 12],
                [377647.0, 3781677.0, 0.00489288, 12],
            ],
            columns=cols
        )
        return df1, df2

    def teardown(self):
        pass

    def test_single(self):
        expected = self.expected_single
        result = Aermod(self.receptor, self.source, quiet=True).runModel()
        assert_frame_equal(expected, result)

    def test_receptors_2(self):
        expected = self.expected_double
        result = Aermod(self.receptors_2, self.source, quiet=True).runModel()
        assert_frame_equal(expected, result)

    def test_sources_additive(self):
        df = self.source
        many_sources = pd.concat([df]*5)
        expected = self.expected_single
        expected['exposure'] *= many_sources.shape[0]
        result = Aermod(self.receptor, many_sources, quiet=True).runModel()
        assert_frame_equal(expected, result, check_less_precise=True)

    def test_SeriesSource(self):
        source = self.source.iloc[0, :]
        result = Aermod(self.receptor, source, quiet=True).runModel()
        expected = self.expected_single
        assert_frame_equal(expected, result)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-v'], exit=False)
