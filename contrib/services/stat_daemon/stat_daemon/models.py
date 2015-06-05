import copy
import datetime
import glob
import imp
import json
import logging
import math
import os
import random
import time
import sys

from airflow.hooks import MySqlHook, SqliteHook
import pandas as pd

import util

class MetadataTable(object):

    """
    Interface to the metadata view
    """

    def __init__(self, table_name, sql_conn_id='airflow_default'):
        self.table_name = table_name
        self.sql_conn_id = sql_conn_id

    def get_path(self, type, path):
        """
        Returns the path in the table (combination of data type and path)
        """
        if type == '':
            return path
        else:
            return '{}:{}'.format(type, path)

    def get_distinct_stats(self, path, limit=100):
        """
        """
        table = self.table_name
        sql = """\
        SELECT
            DISTINCT(stat) AS stats
        FROM
            v_{table}
        WHERE
            path LIKE '%{path}%'
        LIMIT
            {limit}
        ;
        """.format(**locals())
        logging.info("Executing SQL: " + sql)
        db = util.get_sql_hook(self.sql_conn_id)
        return db.get_pandas_df(sql)


    def get_distinct_paths(self, substr, limit=2500):
        """
        """
        db = util.get_sql_hook(self.sql_conn_id)
        if self.is_mysql():
            sql = """
            SELECT
                SUBSTRING_INDEX(s.name, '{substr}', 1) AS paths
                , COUNT(1) AS n
            FROM (
                SELECT
                    name
                FROM
                    metadata_paths
                ORDER BY
                    RAND()
                LIMIT {limit}
            ) s
            GROUP BY
                SUBSTRING_INDEX(s.name, '{substr}', 1)
            ORDER BY
                COUNT(1) DESC
            ;
            """.format(**locals())
            logging.info("Executing SQL: " + sql)
            return db.get_pandas_df(sql)
        else:
            sql = """
            SELECT
                name AS paths
            FROM
                metadata_paths
            ORDER BY
                RANDOM()
            LIMIT {limit}
            ;
            """.format(**locals())
            logging.info("Executing SQL: " + sql)
            df = db.get_pandas_df(sql)
            paths = {}
            for path in df.paths:
                stem = path.split(substr)[0]
                paths[stem] = paths.get(stem, 0) + 1
            return pd.DataFrame(paths.items(), columns=['paths', 'n'])

    def delete_records(self, path, type='', filters=None):
        """
        Deletes records
        """
        table = self.table_name
        path = self.get_path(type, path)
        db = util.get_sql_hook(self.sql_conn_id)
        sql = """\
        DELETE FROM
            {table}
        WHERE
            path_id IN (
                SELECT
                    id
                FROM
                    {table}_paths
                WHERE
                    name = '{path}'
            );
        """
        if filters:
            sql += " AND {filters}"
        sql = (sql + ";").format(**locals())
        logging.info("Executing SQL: " + sql)
        db.run(sql)

    def get_records(self, where=None, limit=10000):
        """
        Returns records in a dataframe
        """
        where = where or ''
        db = util.get_sql_hook(self.sql_conn_id)
        table = self.table_name
        if where.replace(' ', ''):
            where = "WHERE " + where
        sql = """\
        SELECT
            *
        FROM
            v_{table}
        {where}
        LIMIT
            {limit}
        ;
        """.format(**locals())
        logging.info("Executing SQL: " + sql)
        return db.get_pandas_df(sql)

    def __insert_paths_sql(self, paths):
        """
        Generate insert statement
        """
        values = ','.join(["('{}')".format(path) for path in paths])
        table = self.table_name
        sql = ""
        if self.is_mysql():
            return """\
            INSERT INTO {table}_paths 
            (name) VALUES {values} ON DUPLICATE KEY UPDATE name=name;
            """.format(**locals())
        else:
            return """\
            INSERT OR REPLACE INTO {table}_paths 
            (name) VALUES {values};
            """.format(**locals())

    def insert_rows(self, rows):
        """
        Returns the underlying hook
        """
        if not rows:
            return
        db = util.get_sql_hook(self.sql_conn_id)
        table = self.table_name
        paths = set([row[0] for row in rows])
        sql = self.__insert_paths_sql(paths)
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)
        names = ', '.join(["'{}'".format(p) for p in paths])
        sql = """\
        SELECT
            id
            , name
        FROM
            {table}_paths
        WHERE
            name IN ({names})
        ;
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)
        data = db.get_records(sql)
        path_to_id = {path: id for id, path in data}
        rows_mapped = [[path_to_id[row[0]]] + row[1:] for row in rows]
        db.insert_rows(self.table_name, rows_mapped)

    def create_test_data(self):
        """
        Creates test data
        """
        logging.info("Creating test data.")
        dates = pd.date_range('2011-01-01', datetime.datetime.utcnow())
        n_dates = float(len(dates))
        batch = 100
        # random walk
        rows = []
        val1 = random.random()
        val2 = random.random()
        for i, ds in enumerate(dates):
            val1 += random.random() - 0.5
            val = val1
            if i%100 == 0:
                val = 10
            rows.append(['test:random_walk/' + ds.strftime('%Y-%m-%d'), 
                          'random_walker_1', val, time.time()])
            val = val2
            if random.random() < 0.005:
                val = -10
            val2 += random.random() - 0.5
            rows.append(['test:random_walk/' + ds.strftime('%Y-%m-%d'), 
                          'random_walker_2', val, time.time()])
        for i in range(0, len(rows), batch):
            self.insert_rows(rows[i:min(i+batch, len(rows))])
        # random noise
        rows = []
        for ds in dates:
            val = random.random()-0.5
            if random.random() < 0.005:
                val *= 3.0
            rows.append(['test:noise/' + ds.strftime('%Y-%m-%d'), 
                          'noise_1', val, time.time()])
            val = 10 + random.random()-0.5
            if random.random() < 0.005:
                val -= 2.0
            rows.append(['test:noise/' + ds.strftime('%Y-%m-%d'), 
                          'noise_2', val, time.time()])
        for i in range(0, len(rows), batch):
            self.insert_rows(rows[i:min(i+batch, len(rows))])
        # random noise with trend
        rows = []
        for i, ds in enumerate(dates):
            val = i*5.0/len(dates) + (random.random()-0.5)
            if random.random() < 0.01:
                val += val*0.25*(random.random()-0.5)
            rows.append(['test:noisy_trend/' + ds.strftime('%Y-%m-%d'), 
                          'trend_1', val, time.time()])
            if random.random() < 0.005:
                val = val*(2.0*random.random())
            val = 5.0*math.sin(math.pi*i/30.0) + random.random()-0.5
            if random.random() < 0.0025:
                val = val*(0.1*random.random())
            rows.append(['test:noisy_trend/' + ds.strftime('%Y-%m-%d'), 
                          'trend_2', val, time.time()])
        for i in range(0, len(rows), batch):
            self.insert_rows(rows[i:min(i+batch, len(rows))])
        # seasonal trend
        rows = []
        for i, ds in enumerate(dates):
            val = math.pow(i/n_dates, 3.0)
            val += 0.1*val*math.sin(math.pi*i/7.0)
            val += 0.025*val*(random.random()-0.5)
            if random.random() < 0.005:
                val = val*(2.0*random.random())
            rows.append(['test:seasonal_trend/' + ds.strftime('%Y-%m-%d'), 
                          'weekly_seasonality', val, time.time()])
            val = math.pow(i/n_dates, 3.0)
            val += val*(1.0 + math.sin(math.pi*i/365.0))
            val += 0.1*val*math.sin(math.pi*i/7.0)
            val += 0.025*val*(random.random()-0.5)
            if random.random() < 0.005:
                val = val*(2.0*random.random())
            rows.append(['test:seasonal_trend/' + ds.strftime('%Y-%m-%d'), 
                          'weekly_yearly_seasonality', val, time.time()])
        for i in range(0, len(rows), batch):
            self.insert_rows(rows[i:min(i+batch, len(rows))])



    def is_mysql(self):
        """
        Attempts to guess whether the connection is mysql
        """
        if 'sqlite' in self.sql_conn_id:
            return False
        else:
            return True

    def __path_table_sql(self):
        """
        Some slight difference between MySQL and sqlite
        """
        table = self.table_name
        if self.is_mysql():
            return """\
            CREATE TABLE IF NOT EXISTS {table}_paths (
                id      BIGINT  NOT NULL AUTO_INCREMENT,
                name    TEXT NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (name(256))
            );
            """.format(**locals())
        else:
            return """\
            CREATE TABLE IF NOT EXISTS {table}_paths (
                id      INTEGER PRIMARY KEY,
                name    TEXT NOT NULL,
                UNIQUE (name)
            );
            """.format(**locals())


    def create_table(self, drop=False, with_test_data=False):
        """
        Creates the metadata table
        """
        db = util.get_sql_hook(self.sql_conn_id)
        table = self.table_name
        if drop:
            sql = "DROP TABLE IF EXISTS {table};".format(**locals())
            logging.info("Executing SQL: \n" + sql)
            db.run(sql)
            sql = "DROP TABLE IF EXISTS {table}_paths;".format(**locals())
            logging.info("Executing SQL: \n" + sql)
            db.run(sql)
            sql = "DROP VIEW IF EXISTS v_{table};".format(**locals())
            logging.info("Executing SQL: \n" + sql)
            db.run(sql)
        sql = self.__path_table_sql()
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)
        index = ', INDEX path_id_FK (path_id)' if self.is_mysql() else ''
        sql = """\
        CREATE TABLE IF NOT EXISTS {table} (
            path_id BIGINT NOT NULL,
            stat    CHAR(64) NOT NULL,
            val     FLOAT,
            ts      INT
            {index}
        );
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)
        sql = """
        DROP VIEW IF EXISTS v_{table};
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)        
        sql = """\
        CREATE VIEW v_{table} AS
        SELECT
            p.name AS path
            , s.stat
            , s.val
            , s.ts
        FROM
            {table} s
        JOIN
            {table}_paths p
        ON
            s.path_id = p.id
        ;        
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)
        if with_test_data:
            self.create_test_data()




class BaseTable(object):
    
    """
    Contains tags for outliers
    """

    def __init__(self, table_name, sql_conn_id='airflow_default'):
        """
        """
        self.table_name = table_name
        self.sql_conn_id = sql_conn_id

    def create_table_schema(self, schema=None, drop=False):
        """
        """
        if not schema:
            logging.error("Table schema undefined")
        db = util.get_sql_hook(self.sql_conn_id)
        table = self.table_name
        if drop:
            sql = "DROP TABLE IF EXISTS {table};".format(**locals())
            logging.info("Executing SQL: \n" + sql)
            db.run(sql)
        sql = "CREATE TABLE IF NOT EXISTS {table} (" + schema + " );"
        sql = sql.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)

    def insert_rows(self, rows):
        """
        """
        db = util.get_sql_hook(self.sql_conn_id)
        db.insert_rows(self.table_name, rows)        

    def delete_records(self, where=None):
        """
        """
        if not where:
            return
        table = self.table_name
        sql = """
        DELETE FROM
            {table}
        WHERE
            {where}
        ;
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db = util.get_sql_hook(self.sql_conn_id)
        db.run(sql)


    def get_records(self, where=None, limit=10000):
        """
        Returns records from the table
        """
        table = self.table_name
        where = where or ''
        if where.replace(' ', ''):
            where = "WHERE " + where
        sql = """
        SELECT
            *
        FROM
            {table}
        {where}
        LIMIT
            {limit}
        ;
        """.format(**locals())
        logging.info("Executing SQL: \n" + sql)
        db = util.get_sql_hook(self.sql_conn_id)
        return db.get_pandas_df(sql)



class MetadataTags(BaseTable):
    
    """
    Contains tags for outliers
    """

    def __init__(self, table_name, sql_conn_id='airflow_default'):
        """
        """
        super(MetadataTags, self).__init__(
            table_name=table_name + "_tags",
            sql_conn_id=sql_conn_id)

    def create_table(self, drop=False):
        """
        """
        schema = """
            path        TEXT NOT NULL,
            stat        CHAR(64),
            category    INTEGER,
            author      CHAR(64),
            comment     TEXT,
            ts          INT
        """
        self.create_table_schema(schema, drop)

 
class MetadataAlerts(BaseTable):
    """
    Contains alerts that are triggered when outliers are detected
    """

    def __init__(self, table_name, sql_conn_id='airflow_default'):
        """
        """
        super(MetadataAlerts, self).__init__(
            table_name=table_name + "_alerts",
            sql_conn_id=sql_conn_id)

    def create_table(self, drop=False):
        """
        """
        schema = """
            path            TEXT NOT NULL,
            stat            CHAR(64),
            email           TEXT,
            level           INTEGER,
            subject         CHAR(128),
            message         TEXT,
            detrend         TEXT,
            ts              INT
        """
        self.create_table_schema(schema, drop)
