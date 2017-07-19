import numpy as np

import nose
from numpy.testing import assert_array_equal, assert_allclose

from atmods.kernels import polar_kernel, bikernel, _clean_coords


class TestKerns(object):

    @classmethod
    def setup_class(cls):
        cls.x = np.arange(5)
        cls.y = np.arange(5) + 1

    def test_cleancoords_simple(self):
        expected = (self.x, self.y)
        result = _clean_coords(self.x, self.y)
        assert_array_equal(np.column_stack(expected),
                           np.column_stack(result))

    def test_cleancoords_1array(self):
        expected = np.column_stack((self.x, self.y))
        result = _clean_coords(expected, None)
        assert_array_equal(expected, np.column_stack(result))

    def test_cleancoords_1arr_center(self):
        center = (3, 4)
        X = np.column_stack((self.x, self.y))
        expected = np.column_stack((X[:, 0] - center[0],
                                    X[:, 1] - center[1]))
        result = _clean_coords(X, center=center)
        assert_array_equal(expected, np.column_stack(result))

    def test_bivar_unif1(self):
        h = 1
        expected = np.zeros(len(self.x))
        expected[0] = 1 / (h * 2.)
        result = bikernel(self.x, self.y, 1, 'unif')
        assert_array_equal(expected, result)

    def test_polar_unif1(self):
        h = 1
        expected = np.zeros(len(self.x))
        expected[0] = 1 / (h**2 * np.pi)
        result = polar_kernel(self.x, self.y, h, 'unif')
        assert_array_equal(expected, result)

    def test_polar_unif2(self):
        h = 2
        expected = np.zeros(len(self.x))
        expected[0] = 1 / (h**2 * np.pi)
        result = polar_kernel(self.x, self.y, h, 'unif')
        assert_array_equal(expected, result)

    def test_polar_tria2(self):
        h = 2.
        rnorm = np.sqrt((self.x/h) ** 2 + (self.y/h) ** 2)
        expected = np.maximum(1 - rnorm, 0) * 3 / (h**2 * np.pi)
        result = polar_kernel(self.x, self.y, h, 'tria')
        assert_array_equal(expected, result)

    def test_polar_triw2(self):
        h = 2.
        rnorm = np.sqrt((self.x/h) ** 2 + (self.y/h) ** 2)
        expected = ((rnorm <= 1) * (1 - rnorm ** 2) ** 3
                    * 4 / (h**2 * np.pi))
        result = polar_kernel(self.x, self.y, h, 'triw')
        assert_allclose(expected, result)  # At least to 1e-10


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-v', '--pdb'], exit=False)
