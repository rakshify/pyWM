import datetime
import numbers

from functools import reduce, partial

import numpy as np
import pandas as pd

from lib.op_utils.basic_utils import method_map as core_method_map


method_map = {}
method_map.update(core_method_map)


def mapper_method(*args, **kwargs):
    def map_str(k, mapper):
        try:
            for arg in k.split('.'):
                if mapper == "":
                    return mapper
                mapper = mapper[arg]
            return mapper
        except KeyError:
            return ""  # TODO: check how to handle this

    if isinstance(args[0], pd.Series):
        return args[0].map(kwargs["map"], na_action="")
    if isinstance(args[0], str):
        return map_str(args[0], kwargs["map"])
    raise TypeError("Method not supported for %s." % type(args[0]))


def slice_(*args, **kwargs):
    if isinstance(args[0], pd.Series):
        return args[0].str.slice(**kwargs)
    if isinstance(kwargs["v1"], str):
        return args[0][kwargs["start"]:kwargs["stop"]]


def lslice(*args, **kwargs):
    return slice_(start=0, *args, **kwargs)


def rslice(*args, **kwargs):
    if isinstance(kwargs["v1"], pd.Series):
        if "step" not in kwargs:
            return args[0].str.slice(start=-kwargs["start"])
        else:
            return args[0].str.slice(start=-kwargs["start"],
                                     step=kwargs["step"])
    if isinstance(args[0], str):
        return args[0][-kwargs["start"]:]


method_map = {
    "str": lambda *args, **kwargs: args[0].map(str).str.strip()
    if isinstance(args[0], pd.Series()) else str(args[0]),
    "map": mapper_method,
    "slice": slice_,
    "lslice": lslice,
    "rslice": rslice
}


# COMPOUND FUNCTIONS

def date_diff(*args, **kwargs):
    # Function to calculate the difference between dates.
    # First argument is pandas Series containing dates from which to subtract,
    # Second argument is pandas Series containing dates to subtract and
    # Third argument is the kind of conversion needed for
    # final output day/month/etc
    for i in ("start", "end"):
        try:
            dt = kwargs[i].map(int)
            kwargs[i] = method_map["excel_int_to_date"](date=dt)
        except TypeError:
            kwargs[i] = method_map["str_to_date"](date=kwargs[i],
                                                  format="%m/%d/%Y")
    res = method_map["sub"](kwargs["end"], kwargs["start"])
    op_format = "timedelta_to_%s" % kwargs["delta"]
    return method_map[op_format](delta=op_format)


def conditional(*args, **kwargs):
    # Function to apply an if-else conditional. Make sure all args are
    # pandas Series with first arg as the condition, second arg as the value
    # to return for true third arg is the value to return for false
    def apply_condition(row):
        return row["true"] if row["condition"] else row["false"]

    df = pd.DataFrame(kwargs)
    return df.apply(apply_condition, axis=1)


method_map.update({
    "conditional": conditional,
    "date_diff": date_diff
})


# CROSS FRAME FUNCTIONS
def _intersect_frame(df1, df2, on, left_on=None, right_on=None):
    def fix_join_col(df, old, new):
        df[new] = df[old].map(str)
        return df.drop(old, axis=1)

    if left_on:
        df1 = fix_join_col(df1, left_on, on)
    if right_on:
        df2 = fix_join_col(df2, right_on, on)
    df = df1.merge(df2, on=on, suffixes=('_left', '_right'))
    cols = df.columns
    for col in cols:
        if col.endswith("_left"):
            df[col.replace("_left", "")] = df[col]
        if col.endswith("_left") or col.endswith("_right"):
            df = df.drop(col, axis=1)
    return df


def intersect(*args, **kwargs):
    df1 = args[0]
    df2 = args[1]
    if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
        return _intersect_frame(df1, df2, **kwargs)
    err = "Can not intersect %s with %s as of now." % (type(df1), type(df2))
    raise TypeError(err)


method_map.update({
    "intersect": intersect
})
