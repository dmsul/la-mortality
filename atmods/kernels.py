import numpy as np

KERNELS = ('unif', 'tria', 'triw', 'epan')


def polar_kernel(x, y=None, h=None, kernname=None, center=None):
    """Polar kernel density of type `kernname`."""
    _check_parameters(h, kernname)
    # Weights for polar kernels
    polar_weights = {'uniform': 1,
                     'triangle': 3,
                     'epanechnikov': 2,
                     'triweight': 4}
    std_kern_name = _standardize_kernname(kernname)
    normalization_constant = polar_weights[std_kern_name] / np.pi
    kernel = _kernel(std_kern_name)
    # Normalize coordinates
    xtilde, ytilde = _clean_coords(x, y, center)
    r_norm = np.sqrt(xtilde**2 + ytilde**2)/float(h)
    in_bandwidth = (r_norm <= 1)

    density = (kernel(r_norm)
               * normalization_constant * in_bandwidth / h**2)

    return density


def bikernel(x, y=None, h=None, kernname=None, center=None):
    """Bivariate kernel densities."""
    _check_parameters(h, kernname)
    # Weights for bivariate kernels
    bi_weights = {'uniform':       .5,
                  'triangle':       1,
                  'epanechnikov':   9./16,
                  'triweight':      (35./32)**2}

    std_kern_name = _standardize_kernname(kernname)
    normalization_constant = bi_weights[std_kern_name]
    kern = _kernel(std_kern_name)
    # Normalize coordinates
    xtilde, ytilde = _clean_coords(x, y, center)
    xtilde, ytilde = np.abs(xtilde/h), np.abs(ytilde/h)
    in_bandwidth = np.logical_and(xtilde <= 1, ytilde <= 1)

    density = (kern(xtilde) * kern(ytilde)
               * normalization_constant * in_bandwidth / h**2)

    return density


def _check_parameters(h, kernname):
    if h is None:
        raise ValueError("Must pass bandwidth.")
    elif kernname not in KERNELS:
        raise ValueError("Kernel '{}' invalid.".format(kernname))


def _clean_coords(x, y=None, center=None):
    if x.ndim == 2 and y is None:
        if hasattr(x, 'values'):
            arr = x.values
        else:
            arr = x
        xvec, yvec = arr[:, 0], arr[:, 1]
    elif x.ndim == 1 and y.ndim == 1:
        xvec, yvec = x, y
    else:
        err_str = "Too many dimensions: {} X, {} Y".format(x.ndim, y.ndim)
        raise ValueError(err_str)

    if center is not None:
        if len(center) != 2:
            errstr = "Center must be length 2, not {}.".format(len(center))
            raise ValueError(errstr)

        xtilde = xvec - center[0]
        ytilde = yvec - center[1]
    else:
        xtilde, ytilde = xvec, yvec

    return xtilde, ytilde


def _standardize_kernname(kernname):
    if kernname in 'tri':
        raise ValueError('Kernel {} is ambiguous.'.format(kernname))
    elif kernname in 'uniform':
        return 'uniform'
    elif kernname in 'triangle':
        return 'triangle'
    elif kernname in 'epanechnikov':
        return 'epanechnikov'
    elif kernname in 'triweight':
        return 'triweight'
    else:
        raise ValueError("Kernel '{}' not supported.".format(kernname))


def _kernel(KERNEL):
    """Defines kernel up to re-weighting constant, which varies with use
    (constant will be different in univariate, bivariate, polar, etc.)."""
    if KERNEL == 'uniform':
        def _kern(x):
            return 1
    elif KERNEL == 'triangle':
        def _kern(x):
            return 1 - x
    elif KERNEL == 'epanechnikov':
        def _kern(x):
            return (1 - x**2)
    elif KERNEL == 'triweight':
        def _kern(x):
            return (1 - x**2)**3
    else:
        raise ValueError("Kernel name '{}' not standardized!")

    return _kern
