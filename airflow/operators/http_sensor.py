from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow import settings
from airflow.models import Connection as DB
import logging

class HTTPSensor(BaseSensorOperator):
    """
    Waits for a queue RabbitMQ length to be below min_count

    :param conn_id: A reference to a RabbitMQ connection
    :type conn_id: str
    :param queue_name: Name of the queue to get length
    :type queue_name: str
    :param min_count: Minimum queue length to trigger senson
    :type min_count: int
    """

    @apply_defaults
    def __init__(self, conn_id, path, *args, **kwargs):

        super(HTTPSensor, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.path = path

        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        logging.info('Found DB', extra=dict(conn_id=db.conn_id, conn_type=db.conn_type))
        self.hook = db.get_hook()
        session.commit()
        session.close()

    def poke(self):
        logging.info('Poking: ' + self.path)
        response = self.hook.GET(self.path)
        logging.info('Response: ' + str(response))
        try:
            response.index('ERROR:')
            raise Exception(response)
        except ValueError:
            pass
        if response[-1] == '0':
            return True
        else:
            return False

