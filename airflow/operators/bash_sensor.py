from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow import settings
from airflow.models import Connection as DB
import logging

class BashSensor(BaseSensorOperator):
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
    def __init__(self, bash_command, env=None, *args, **kwargs):

        super(BashSensor, self).__init__(*args, **kwargs)

        self.bash_command = bash_command
        self.env = env

    def poke(self):
        from subprocess import Popen, STDOUT, PIPE
        logging.info('Poking: ' + self.bash_command)
        sp = Popen(self.bash_command, env=self.env, shell=True, stdout=PIPE, stderr=STDOUT)
        stdout, stderr = sp.communicate()
        logging.info('stdout: ' + str(stdout))
        logging.info('stderr: ' + str(stderr))
        if sp.returncode == 0:
            try:
                stdout.index('ERROR:')
                raise Exception(stdout)
            except ValueError:
                pass
            # remove empty lines for output
            lines = stdout.strip().split('\n')
            logging.info('result: ' + lines[-1])
            if lines[-1] == '0':
                return True
            else:
                return False
        else:
            raise Exception('Return code: ' + str(sp.returncode))


