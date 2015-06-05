import copy
import datetime
import glob
import imp
import json
import logging
import os
import random
import time
import sys

from airflow.hooks import HDFSHook, MySqlHook, PrestoHook, SqliteHook
import util

class Reporter(object):

    """
    Reports on the status of data
    """

    def __init__(self, args):
        pass

    def get_updated_timestamps(self):
        """
        Gets the metadata
        """
        raise NotImplementedError

    def __map_path_to_ts(self, obj):
        """
        Returns updated at times
        """
        updated_at = {}
        for item in obj.get_updated_timestamps():
            updated_at[item['path']] = item['ts']
        return updated_at

    def compare_to(self, other):
        """
        Compares two sets of data and determines what to update
        """
        self_updated_at = self.__map_path_to_ts(self)
        other_updated_at = self.__map_path_to_ts(other)
        update = []
        for item in other.get_updated_timestamps():
            # items that don't yet exist
            if item['path'] not in self_updated_at:
                update.append(item['path'])
            # items that are stale
            elif item['ts'] > self_updated_at[item['path']]:
                update.append(item['path'])
        delete = []
        for item in self.get_updated_timestamps():
            if item['path'] not in other_updated_at:
                delete.append(item['path'])
        return update, delete


class MetadataReporter(Reporter):

    """
    Interface to the metadata table
    """

    def __init__(
            self, table_name, path,
            sql_conn_id='airflow_default'):
        self.table_name = table_name
        self.sql_conn_id = sql_conn_id
        self.path = path

    def get_updated_timestamps(self):
        """
        """
        db = util.get_sql_hook(self.sql_conn_id)
        path = self.path.replace('*', '%')
        sql = """\
        SELECT
              path
            , MIN(ts)
        FROM
            v_{table}
        WHERE
            path LIKE '%{path}'
        GROUP BY
            path
        ;
        """.format(table=self.table_name, path=path)
        logging.info("Executing SQL: \n" + sql)
        data = db.get_records(sql)
        return [{'path': ':'.join(item[0].split(':')[1:]),
                 'ts': item[1]} for item in data]


class LocalFsReporter(Reporter):

    def __init__(self, path):
        self.path = path

    def get_updated_timestamps(self):
        """
        Get timestamps for files
        """
        data = []
        for item in glob.glob(self.path):
            try:
                data.append({'path': item, 'ts': os.stat(item)[8]})
            except:
                logging.error("Failed to stat {}.".format(item))
                pass
        return data


class HdfsReporter(Reporter):

    """
    Reports the updated at timestamps for data in hdfs
    """

    def __init__(self, path, hdfs_conn_id='hdfs_default'):
        self.path = path
        self.hdfs_conn_id = hdfs_conn_id

    def get_updated_timestamps(self):
        """
        Get the updated_at timestamp of each file
        """
        try:
            hdfs = HDFSHook(self.hdfs_conn_id).get_conn()
            data = [{'path': item['path'], 
                        'ts':item['modification_time']/1000} for
                    item in hdfs.ls([self.path],
                                    recurse=False,
                                    include_toplevel=True,
                                    include_children=False)]
            return data
        except:
            logging.error("HdfsReporter: Failed to stat {}".format(self.path))
            return []


class HiveReporter(Reporter):

    def __init__(self, path, metastore_mysql_conn_id='metastore_mysql'):
        self.metastore_conn_id = metastore_mysql_conn_id
        self.path = path
        self.db = 'default'
        parts = self.path.split('.')
        self.table_name = parts[0]
        self.partition_expr = None
        if len(parts) == 2:
            self.db, self.table_name = parts
            parts = self.table_name.split('/')
            if len(parts) == 2:
                self.table_name = parts[0]
                self.partition_expr = '/'.join(parts[1:])

    def get_updated_timestamps(self):
        """
        Query the metastore for partition landing times
        """
        table_name = self.table_name
        partition_expr = self.partition_expr
        db = self.db
        sql = """\
        SELECT
        """
        if partition_expr:
            sql += """
              p.PART_NAME
            , p.CREATE_TIME
            """
        else:
            sql += """
              NULL
            , t.CREATE_TIME
            """
        sql += """
        FROM (
            SELECT
                n.TBL_ID
                , n.TBL_NAME
                , n.CREATE_TIME
            FROM (
                SELECT
                    TBL_NAME
                    , TBL_ID
                    , DB_ID
                    , CREATE_TIME
                FROM
                    metastore.TBLS
                WHERE
                    TBL_NAME = '{table_name}'
            ) n
            INNER JOIN (
                SELECT
                    DB_ID
                FROM
                    metastore.DBS
                WHERE
                    NAME = '{db}'
            ) d
            ON
                n.DB_ID = d.DB_ID
        ) t
        """
        if partition_expr:
            sql += """
        INNER JOIN
            metastore.PARTITIONS p
        ON
            p.TBL_ID = t.TBL_ID
        WHERE
            p.PART_NAME LIKE '{partition_expr}'
        ;
        """
        sql = sql.format(**locals())
        logging.info("Querying metastore: " + sql)
        try:
            metastore = MySqlHook(mysql_conn_id=self.metastore_conn_id)
            rows = metastore.get_records(sql)
            data = []
            for row in rows:
                path = '{}.{}'.format(self.db, self.table_name)
                if partition_expr:
                    path += '/{}'.format(row[0])
                updated_at = row[1]
                data.append({'path': path, 'ts': updated_at})
            return data
        except:
            logging.error("HiveReporter: Failed to stat {}".format(self.path))
            return []


class MySqlReporter(Reporter):
    # TODO
    pass


class S3Reporter(Reporter):
    # TODO
    pass
