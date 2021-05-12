from chispa.row_comparer import are_rows_equal
from pyspark.sql import Row
from pyspark.sql.types import *
import pytest


@pytest.fixture(params=['float', 'double', 'decimal'])
def float_str_dtypes(request):
    return [request.param, 'string']


def describe_are_rows_equal():
    def returns_False_when_string_values_are_not_equal():
        assert not are_rows_equal(
            Row(n1="bob", n2="jose"),
            Row(n1="li", n2="li"),
            dtypes=['string', 'string'],
        )
    def returns_True_when_string_values_are_equal():
        assert are_rows_equal(
            Row(n1="luisa", n2="laura"),
            Row(n1="luisa", n2="laura"),
            dtypes=['string', 'string'],
        )
    def returns_True_when_both_rows_are_None():
        assert are_rows_equal(
            Row(n1=None, n2=None),
            Row(n1=None, n2=None),
            dtypes=['string', 'string'],
        )


def describe_are_rows_equal_when_allowing_nan_equality():
    def returns_False_when_no_NaN_values_to_compare_and_other_values_are_not_equal():
        assert not are_rows_equal(
            Row(n1="bob", n2="jose"),
            Row(n1="li", n2="li"),
            dtypes=['string', 'string'],
            allow_nan_equality=True,
        )
    def returns_True_when_either_the_values_are_equal_or_both_nan(float_str_dtypes):
        assert are_rows_equal(
            Row(n1=float('nan'), n2="jose"),
            Row(n1=float('nan'), n2="jose"),
            dtypes=float_str_dtypes,
            allow_nan_equality=True,
        )
    # NOTE: This test might not be needed as the schema would have failed before.
    def returns_False_when_comparing_nan_to_string(float_str_dtypes):
        assert not are_rows_equal(
            Row(n1=float('nan'), n2="jose"),
            Row(n1="hi", n2="jose"),
            dtypes=float_str_dtypes,
            allow_nan_equality=True,
        )


def describe_are_rows_equal_when_given_precision():
    def returns_True_when_float_value_difference_is_less_than_precision(float_str_dtypes):
        assert are_rows_equal(
            Row(num = 1.1, first_name = "li"),
            Row(num = 1.05, first_name = "li"),
            dtypes=float_str_dtypes,
            precision=0.1,
        )
    def returns_True_when_float_values_are_exactly_equal(float_str_dtypes):
        assert are_rows_equal(
            Row(num = 5.0, first_name = "laura"),
            Row(num = 5.0, first_name = "laura"),
            dtypes=float_str_dtypes,
            precision=0.1,
        )
    def returns_False_when_float_value_difference_is_more_than_precision(float_str_dtypes):
        assert not are_rows_equal(
            Row(num = 5.0, first_name = "laura"),
            Row(num = 5.9, first_name = "laura"),
            dtypes=float_str_dtypes,
            precision=0.1,
        )
    def returns_True_when_all_values_are_Nones(float_str_dtypes):
        assert are_rows_equal(
            Row(num = None, first_name = None),
            Row(num = None, first_name = None),
            dtypes=float_str_dtypes,
            precision=0.1,
        )

