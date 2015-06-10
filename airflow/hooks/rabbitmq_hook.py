from airflow.hooks.base_hook import BaseHook
from librabbitmq import Connection


class RabbitMQHook(BaseHook):
    """
    Interact with RabbitMQ.
    """
    def __init__(
            self, rabbitmq_conn_id='rabbitmq_default'):
        self.rabbitmq_conn_id = rabbitmq_conn_id

    def get_conn(self):
        """
        Returns a RabbitMQ connection object
        """
        conn = self.get_connection(self.rabbitmq_conn_id)
        port = 5672
        if conn.port:
            port = conn.port
        conn = Connection(host=conn.host, userid=conn.login, password=conn.password, virtual_host=conn.schema,
                          port=port)
        return conn

    def get_queue_length(self, queue_name):
        conn = self.get_conn()
        channel = conn.channel()
        queue = channel.queue_declare(queue_name, durable=True)
        return queue.message_count