import datetime
import json
import numpy as np
import pandas as pd

def unix_time(dt):
    """
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def df_to_series(df, points=False):
    """
    """
    series = []
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            if col not in ['Tags']:
                data = []
                for i, item in enumerate(df[col]):
                    #x = int(pd.to_datetime(df.index[i]).strftime("%s"))*1000
                    x = unix_time(pd.to_datetime(df.index[i]))*1000
                    if not np.isnan(item):
                        data.append((x, item))
                options = {'name': col, 'data': data, 'id': col}
                if col in ['min_tol', 'max_tol']:
                    options['dashStyle'] = 'longdash'
                series.append(options)
    return series


def get_flags(data, name, on_series=''):
    """
    """
    flags = {'type': 'flags', 'data': [],
             'onSeries': on_series,
             'name': name,
             'shape': 'squarepin',
             'width': 12}
    for x, y in data:
        flags['data'].append({
            'x': x, 'title': '!', 'text': 'value: ' + str(y)
        })
    return [flags]


def get_outliers(df):
    """
    """
    if not ('max_tol' in df.columns or 'min_tol' in df.columns):
        return []
    outliers = []
    col = 'Error' if 'Error' in df.columns else df.columns[0]
    for i, item in enumerate(df[col]):
        idx = unix_time(pd.to_datetime(df.index[i]))*1000
        if 'max_tol' in df.columns:
            if item > df['max_tol'][i]:
                outliers.append([idx, item])
        if 'min_tol' in df.columns:
            if item < df['min_tol'][i]:
                outliers.append([idx, item])
    return outliers

def get_tags(df):
    """
    """
    tags = []
    if not 'Tags' in df.columns:
        return tags
    for i, item in enumerate(df['Tags']):
        if item:
            idx = unix_time(pd.to_datetime(df.index[i]))*1000
            tags.append([idx, item])
    return tags

def get_series(df, data):
    """
    """
    options = {'name': 'outliers', 'data': data}
    options['marker'] = {'enabled': True, 'radius': 3}
    options['line'] = {'enabled': False}
    return [options]

def highchart_timeseries(df, title=''):
    """
    """
    series = df_to_series(df)
    outliers = get_series(df, get_outliers(df))
    oncol = 'Error' if 'Error' in df.columns else df.columns[0]
    if outliers:        
        series += get_flags(outliers[0]['data'], 'outliers', oncol)
    tags = get_series(df, get_tags(df))
    if tags:
        series += get_flags(tags[0]['data'], 'tags', oncol)
    chart = {
        'title': {
            'text': title
        },
        'series': series,
        'legend': {
            'enabled': True,
            'borderWidth': 0
        },
    }
    return json.dumps(chart)
