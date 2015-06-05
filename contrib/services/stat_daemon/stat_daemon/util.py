import datetime
import dateutil.parser
import json
import logging
import re

import pandas as pd
from airflow.hooks import MySqlHook, SqliteHook


def get_sql_hook(sql_conn_id):
    """
    Local helper function to get a SQL hook
    """
    if 'sqlite' in sql_conn_id:
        return SqliteHook(sql_conn_id)
    else:
        return MySqlHook(sql_conn_id)


def require_args(args, req):
    """
    Custom require args (args might be modified by code)
    """
    error = None
    msg = "Argument --{} not specified"
    for k, v in vars(args).iteritems():
        if k in req and not v:
            logging.error(msg.format(k))
            error = True
    for item in req:
        if not item in vars(args):
            logging.error(msg.format(item))
            error = True
    if error:
        sys.exit(1)


def update_args(args, task):
    """
    Override arguments
    """
    args_ = copy.deepcopy(args)
    args_.type = task.type
    args_.path = task.path
    if hasattr(task, 'plugin'):
        if task.plugin:
            args_.plugin = task.plugin
    if hasattr(task, 'plugin_args'):
        if task.plugin_args:
            args_.plugin_args = "'{}'".format(json.dumps(task.plugin_args))
    return args_


def load_plugin(path=None):
    """
    load plugin file by string
    """
    plugin = None
    if path:
        try:
            plugin = imp.load_source('plugin', path)
        except Exception as e:
            logging.error("Could not load plugin {}.".format(path))
            logging.error(e)
    return plugin

