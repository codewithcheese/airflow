import datetime
import dateutil.parser
import math
import re

import plugins.tsa.default
import util

from models import MetadataTags, MetadataTable

import pandas as pd


def get_datetime(text):
    """
    Attempt to extract datetime from a text field
    """
    default = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    default.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        ds = dateutil.parser.parse(text, fuzzy=True, default=default)
        if len([m.start() for m in re.finditer(':', text)]) < 2:
            return ds.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return ds
    except:
        return default


def as_series(ts_list):
    """
    Converts a list of tuples to series
    """
    data = pd.Series()
    for ts, val in ts_list:
        data[ts] = val
    return data

def as_dataframe(ts_list):
    """
    Converts a list of tuples to a dataframe
    """
    return pd.DataFrame(as_series(ts_list))


def as_timeseries(df):
    """
    Converts dataframe containing "path" to timeseries
    """
    data = pd.Series()
    if not 'val' in df:
        df['val'] = 0
    for path, val in zip(df.path, df.val):
        ts = get_datetime(path)
        data[ts] = val
    dfts = pd.DataFrame(data)
    return dfts


def as_list(df):
    """
    Converts dataframe timeseries to list
    """
    return [(x, y) for x, y in zip(df.index.tolist(),
                                   df[df.columns[0]].values.tolist())]


def get_data(path,
             stat,
             table=None,
             sql_conn_id='airflow_default',
             start_time='2011-01-01',
             end_time=datetime.datetime.utcnow().strftime('%Y-%m-%d')
             ):
    """
    """
    if not table:
        logging.error("Table not defined.")
        return []
    fmt = '%Y-%m-%d %H:%M:%S'
    where = "path like '{}%' AND stat='{}'".format(path, stat)
    data = as_list(as_timeseries(table.get_records(
        where=where, limit=10000)))
    data = [row for row in data if
            row[0].strftime(fmt) >= start_time and
            row[0].strftime(fmt) <= end_time]
    return data


def get_timeseries(path,
                   stat,
                   table='metadata',
                   sql_conn_id='airflow_default',
                   start_time='2011-01-01',
                   end_time=datetime.datetime.utcnow().strftime('%Y-%m-%d')
                   ):
    """
    """
    metadata_table = MetadataTable(table_name=table,
                                   sql_conn_id=sql_conn_id)
    return get_data(path, stat, metadata_table,
                    sql_conn_id, start_time, end_time)


def get_tags(path,
             stat,
             table='metadata',
             sql_conn_id='airflow_default',
             start_time='2011-01-01',
             end_time=datetime.datetime.utcnow().strftime('%Y-%m-%d')
             ):
    """
    """
    tags_table = MetadataTags(table_name=table,
                              sql_conn_id=sql_conn_id)
    return get_data(path, stat, tags_table,
                    sql_conn_id, start_time, end_time)


def get_error(data, pred):
    """
    """
    err = []
    for y0, yf in zip(data, pred):
        delta = y0[1] - yf[1]
        err.append(
            (y0[0], math.fabs(delta)/(math.fabs(y0[1]) + math.fabs(yf[1]))))
    return err


def detrend(path,
            stat,
            table='metadata',
            sql_conn_id='airflow_default',
            start_time='2011-01-01',
            end_time=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            plugin=None
            ):
    """
    """
    data = get_timeseries(path, stat, table, sql_conn_id, start_time, end_time)
    tags = get_tags(
        path, stat, table, sql_conn_id, start_time, end_time)
    _plugin = util.load_plugin(plugin)
    if not _plugin:
        _plugin = plugins.tsa.default
    fit = _plugin.detrend(data, tags)
    error = get_error(data, fit)
    return data, fit, error, tags
