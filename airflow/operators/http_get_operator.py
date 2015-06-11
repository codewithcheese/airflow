import logging
from airflow import settings
from airflow.models import Connection as DB
from airflow.hooks.http_hook import HTTPHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class HTTPGETOperator(BaseOperator):

    @apply_defaults
    def __init__(self, conn_id, path, *args, **kwargs):

        super(HTTPGETOperator, self).__init__(*args, **kwargs)

        self.path = path
        self.conn_id = conn_id

        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        self.hook = db.get_hook()
        session.commit()
        session.close()

    def execute(self, context):
        logging.info('Executing HTTP GET: ' + self.path)
        hook = HTTPHook(http_conn_id=self.conn_id)
        response = hook.GET(self.path)
        logging.info(response)
        if response[-1] == '0':
            return True
        else:
            raise Exception('HTTP GET returned a non-zero response.')
