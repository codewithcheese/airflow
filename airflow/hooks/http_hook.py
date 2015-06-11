from airflow.hooks.base_hook import BaseHook
import urllib2
import logging
import requests

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
        url = conn.schema + '://' + conn.host + ':' + str(port) + path
        if conn.login:
            auth = (conn.login, conn.password)
        else:
            auth = None

        logging.info('HTTP GET: ' + url)
        response = requests.get(url, verify=False, auth=auth)
        return response.content