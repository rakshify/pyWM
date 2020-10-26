import datetime
import numbers

from functools import reduce, partial

import numpy as np
import pandas as pd

from lib.datasources import get_datasource_by_name
from lib.op_utils.basic_utils import method_map as core_method_map


method_map = {}
method_map.update(core_method_map)


def read_datasource(*args, **kwargs):
    #
    datasource = get_datasource_by_name(args[0])
    collection = args[1]
    filters = kwargs.get("filters", {})
    query_builder = partial(datasource.query_builder.build_query,
                            filters=filters)
    projections = kwargs.get("projections")
    if projections:
        query_builder = partial(query_builder, projections=projections)
    limit = kwargs.get("limit")
    if limit:
        query_builder = partial(query_builder, limit=limit)
    skip = kwargs.get("skip")
    if skip:
        query_builder = partial(query_builder, skip=skip)
    sort = kwargs.get("sort")
    if sort:
        query_builder = partial(query_builder, sort=sort)
    query = query_builder()
    return datasource.read(collection, query)
