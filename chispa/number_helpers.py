import math
from typing import Optional


def isnan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def check_equal(
    x, y,
    dtype_name: str,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> bool:
    """Return True if x and y are equal.

    Parameters
    ----------
    dtype : pyspark DataType
        The type of the values from the schema.
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality: bool, defaults to False
        When True, treats two NaN values as equal.

    """
    is_float_type = (dtype_name in ['float', 'double', 'decimal'])

    if all([i is not None for i in [x, y, precision]]) & is_float_type:
        both_equal = abs(x - y) < precision
    else:
        both_equal = (x == y)

    both_nan = (isnan(x) and isnan(y)) if allow_nan_equality else False

    return both_equal or both_nan
