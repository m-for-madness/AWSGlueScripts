import logging

from airflow.models import BaseOperator
from airflow.operators.python_operator import Py
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class JarOperator:

    @apply_defaults
    def __init__(self, bash_command, *args, **kwargs):
        bash_command = "java -cp /home/madness/airflow/dags/airflowProject.jar " + bash_command
        BashOperator.__init__(bash_command, *args, **kwargs)


class JarOperator(AirflowPlugin):
    name = "jar_operator"
    operators = [JarOperator]
