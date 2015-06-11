from airflow.hooks.base_hook import BaseHook
import urllib2
import logging

class HTTPHook(BaseHook):
    """
    Interact with HTTP api
    """
    def __init__(
            self, http_conn_id='http_default'):
        self.http_conn_id = http_conn_id

    def get_conn(self):
        """
        Returns a RabbitMQ connection object
        """
        conn = self.get_connection(self.http_conn_id)
        return conn

    def GET(self, path):
        conn = self.get_conn()
        port = 80
        if conn.port:
            port = conn.port
        if conn.login:
            url = conn.schema + '://' + conn.login + ':' + conn.password + '@' + conn.host + ':' + str(port) + path
        else:
            url = conn.schema + '://' + conn.host + ':' + str(port) + path
        logging.info('HTTP GET: ' + url)
        response = urllib2.urlopen(url)
        return response.read()