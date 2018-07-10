from airflow.plugins_manager import AirflowPlugin

import AWSGlueJobHook
import AWSGlueJobSensor
from AWSGlueJobOperator import AWSGlueJobOperator


class AwsGlueJobPlugin(AirflowPlugin):
    name = "aws_glue_job_plugin"
    operators = [AWSGlueJobSensor,AWSGlueJobHook, AWSGlueJobOperator]
