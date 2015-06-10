from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow import settings
from airflow.models import Connection as DB
import logging

class RabbitMQSensor(BaseSensorOperator):
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
    def __init__(self, conn_id, queue_name, min_count, *args, **kwargs):

        super(RabbitMQSensor, self).__init__(*args, **kwargs)

        self.queue_name = queue_name
        self.conn_id = conn_id
        self.min_count = min_count

        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        logging.info('Found DB', extra=dict(conn_id=db.conn_id, conn_type=db.conn_type))
        self.hook = db.get_hook()
        session.commit()
        session.close()

    def poke(self):
        logging.info('Poking: ' + self.queue_name)
        count = self.hook.get_queue_length(self.queue_name)
        logging.info('Queue length: ' + str(count))
        if count <= self.min_count:
            return True
        else:
            return False

