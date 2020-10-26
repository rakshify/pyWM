import datetime
import numbers

from functools import reduce, partial

import numpy as np
import pandas as pd

"""
    TODO: MAKE SURE THAT EACH ARG IS A PANDAS SERIES

    Current support only for pandas Series
    TODO: Expand support
"""


class Operation(object):
    @staticmethod
    def pair(a, b, op):
        err = "Base class can not perform pair operations."
        raise NotImplementedError(err)


class BasicOp(Operation):
    pass


class ArithmeticOp(BasicOp):
    @staticmethod
    def pair(a, b, op):
        sa = isinstance(a, pd.Series)
        da = isinstance(a, pd.DataFrame)
        na = isinstance(a, numbers.Number)
        sb = isinstance(b, pd.Series)
        db = isinstance(b, pd.DataFrame)
        nb = isinstance(b, numbers.Number)
        series_op = sa and (sb or nb)
        frame_op = da and (db or nb)
        if series_op or frame_op:
            if op == "add":
                return a.add(b)
            elif op == "sub":
                return a.sub(b)
            elif op == "mul":
                return a.mul(b)
            elif op == "div":
                return a.div(b)
        elif na:
            if op == "add":
                return a + b
            elif op == "sub":
                return a - b
            elif op == "mul":
                return a * b
            elif op == "div":
                return a / b
        raise TypeError(("%s of %s and %s not supported as of now"
                         "." % (op, type(a), type(b))))


class BooleanOp(BasicOp):
    @staticmethod
    def pair(a, b, op):
        if op == "and":
            return a & b
        elif op == "or":
            return a | b
        sa = isinstance(a, pd.Series)
        da = isinstance(a, pd.DataFrame)
        na = isinstance(a, numbers.Number)
        sb = isinstance(b, pd.Series)
        db = isinstance(b, pd.DataFrame)
        nb = isinstance(b, numbers.Number)
        series_op = sa and (sb or nb)
        frame_op = da and (db or nb)
        if series_op or frame_op:
            if op == "lt":
                return a.lt(b)
            elif op == "gt":
                return a.gt(b)
            elif op == "le":
                return a.le(b)
            elif op == "ge":
                return a.ge(b)
            elif op == "ne":
                return a.ne(b)
            elif op == "eq":
                return a.eq(b)
        elif na:
            if op == "lt":
                return a < b
            elif op == "gt":
                return a > b
            elif op == "le":
                return a <= b
            elif op == "ge":
                return a >= b
            elif op == "ne":
                return a != b
            elif op == "eq":
                return a == b
        raise TypeError(("%s of %s and %s not supported as of now"
                         "." % (op, type(a), type(b))))


def _dnc(pair_func, *args, **kwargs):
    l = len(args)
    if l == 0:
        err = "At least one argument must be provided for operation."
        raise ValueError(err)
    if l == 1:
        return args[0]
    m = l // 2
    left = _dnc(*args[:m], op)
    right = _dnc(*args[m:], op)
    return pair_func(left, right)


# BASIC BOOLEAN AND ARITHMETIC OPERATIONS
method_map = {"default": lambda **kwargs: kwargs["v1"]}
OP_MAP = {
    "and": BooleanOp.pair,
    "or": BooleanOp.pair,
    "lt": BooleanOp.pair,
    "gt": BooleanOp.pair,
    "le": BooleanOp.pair,
    "ge": BooleanOp.pair,
    "ne": BooleanOp.pair,
    "eq": BooleanOp.pair,
    "add": ArithmeticOp.pair,
    "sub": ArithmeticOp.pair,
    "mul": ArithmeticOp.pair,
    "div": ArithmeticOp.pair
}
method_map.update({
    op: partial(_dnc, pair_func=partial(OP_MAP[op], op=op))
    for op in ("and", "or", "lt", "le", "gt", "ge", "eq", "ne",
               "add", "sub", "mul", "div")
})


EXCEL_START_DATE = datetime.datetime(1900, 1, 1)
# BASIC DATE-TIME FUNCTIONS


def str_to_date(*args, **kwargs):
    if isinstance(kwargs["date"], pd.Series):
        return pd.to_datetime(kwargs["date"], format=kwargs["format"])
    if isinstance(kwargs["date"], str):
        return datetime.datetime.strptime(kwargs["date"], kwargs["format"])
    raise TypeError("Method not supported for %s." % type(kwargs["date"]))


def excel_int_to_date(*args, **kwargs):
    if isinstance(kwargs["date"], pd.Series):
        return pd.to_timedelta(kwargs["date"], unit='d') + EXCEL_START_DATE
    if isinstance(kwargs["date"], int):
        return EXCEL_START_DATE + datetime.timedelta(days=kwargs["date"])
    raise TypeError("Method not supported for %s." % type(kwargs["date"]))


def timedelta_to_days(*args, **kwargs):
    if isinstance(kwargs["delta"], pd.Series):
        return kwargs["delta"].dt.days
    if isinstance(kwargs["delta"], datetime.timedelta):
        return datetime.timedelta(days=5)
    raise TypeError("Method not supported for %s." % type(kwargs["delta"]))


def timedelta_to_seconds(*args, **kwargs):
    if isinstance(kwargs["delta"], pd.Series):
        return kwargs["delta"].dt.seconds
    if isinstance(kwargs["delta"], datetime.timedelta):
        return datetime.timedelta(seconds=5)
    raise TypeError("Method not supported for %s." % type(kwargs["delta"]))


def timedelta_to_musecs(*args, **kwargs):
    if isinstance(kwargs["delta"], pd.Series):
        return kwargs["delta"].dt.microseconds
    if isinstance(kwargs["delta"], datetime.timedelta):
        return datetime.timedelta(microseconds=5)
    raise TypeError("Method not supported for %s." % type(kwargs["delta"]))


method_map.update({
    "str_to_date": str_to_date,
    "excel_int_to_date": excel_int_to_date,
    "timedelta_to_days": timedelta_to_days,
    "timedelta_to_seconds": timedelta_to_seconds,
    "timedelta_to_musecs": timedelta_to_musecs
})
