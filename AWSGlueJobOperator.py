from __future__ import unicode_literals

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from AWSGlueJobHook import AwsGlueJobHook


class AWSGlueJobOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 job_name='aws_glue_default_job',
                 job_desc='AWS Glue Job with Airflow',
                 script_location=None,
                 concurrent_run_limit=None,
                 script_args={},
                 connections=[],
                 retry_limit=None,
                 num_of_dpus=6,
                 aws_conn_id='aws_default',
                 region_name=None,
                 s3_bucket=None,
                 iam_role_name=None,
                 *args, **kwargs
                 ):
        super(AWSGlueJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_location = script_location
        self.concurrent_run_limit = concurrent_run_limit
        self.script_args = script_args
        self.connections = connections
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.aws_conn_id = aws_conn_id,
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.iam_role_name = iam_role_name

    def execute(self, context):
        glue_job = AwsGlueJobHook(job_name=self.job_name,
                                  desc=self.job_desc,
                                  concurrent_run_limit=self.concurrent_run_limit,
                                  script_location=self.script_location,
                                  conns=self.connections,
                                  retry_limit=self.retry_limit,
                                  num_of_dpus=self.num_of_dpus,
                                  aws_conn_id=self.aws_conn_id,
                                  region_name=self.region_name,
                                  s3_bucket=self.s3_bucket,
                                  iam_role_name=self.iam_role_name)

        self.log.info("Initializing AWS Glue Job: {}".format(self.job_name))
        glue_job_run = glue_job.initialize_job(self.script_args)
        self.log.info('AWS Glue Job: {job_name} status: {job_status}. Run Id: {run_id}'
                      .format(run_id=glue_job_run['JobRunId'],
                              job_name=self.job_name,
                              job_status=glue_job_run['JobRunState'])
                      )

        self.log.info('Done.')