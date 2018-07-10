from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
import os.path
import time


class AwsGlueJobHook(AwsHook):

    def __init__(self,
                 job_name=None,
                 desc=None,
                 concurrent_run_limit=None,
                 script_location=None,
                 conns=None,
                 retry_limit=None,
                 num_of_dpus=None,
                 aws_conn_id='aws_default',
                 region_name=None,
                 iam_role_name=None,
                 s3_bucket=None, *args, **kwargs):
        self.job_name = job_name
        self.desc = desc
        self.concurrent_run_limit = concurrent_run_limit or 1
        self.script_location = script_location
        self.conns = conns or ["s3"]
        self.retry_limit = retry_limit or 0
        self.num_of_dpus = num_of_dpus or 10
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.role_name = iam_role_name
        self.S3_PROTOCOL = "s3://"
        self.S3_ARTIFACTS_PREFIX = 'artifacts/glue-scripts/'
        self.S3_GLUE_LOGS = 'logs/glue-logs/'
        super(AwsGlueJobHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        conn = self.get_client_type('glue', self.region_name)
        return conn

    def list_jobs(self):
        conn = self.get_conn()
        return conn.get_jobs()

    @property
    def get_iam_execution_role(self):
        """
        :return: iam role for job execution
        """
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(self.role_name)
            self.log.info("Iam Role Name: {}".format(self.role_name))
            return glue_execution_role
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def initialize_job(self, script_arguments=None):
        """
        Initializes connection with AWS Glue
        to run job
        :return:
        """
        if self.s3_bucket is None:
            raise AirflowException(
                'Could not initialize glue job, '
                'error: Specify Parameter `s3_bucket`'
            )

        glue_client = self.get_conn()

        try:
            job_response = self.get_or_create_glue_job()
            job_name = job_response['Name']
            job_run = glue_client.start_job_run(
                JobName=job_name,
                Arguments=script_arguments
            )
            return self.job_completion(job_name, job_run['JobRunId'])
        except Exception as general_error:
            raise AirflowException(
                'Failed to run aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def job_completion(self, job_name=None, run_id=None):
        """
        :param job_name:
        :param run_id:
        :return:
        """
        glue_client = self.get_conn()
        job_status = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id,
            PredecessorsIncluded=True
        )
        job_run_state = job_status['JobRun']['JobRunState']
        failed = job_run_state == 'FAILED'
        stopped = job_run_state == 'STOPPED'
        completed = job_run_state == 'SUCCEEDED'

        while True:
            if failed or stopped or completed:
                self.log.info("Exiting Job {} Run State: {}"
                              .format(run_id, job_run_state))
                return {'JobRunState': job_run_state, 'JobRunId': run_id}
            else:
                self.log.info("Polling for AWS Glue Job {} current run state"
                              .format(job_name))
                time.sleep(6)

    def get_or_create_glue_job(self):
        glue_client = self.get_conn()
        try:
            self.log.info("Now creating and running AWS Glue Job")
            s3_log_path = "s3://{bucket_name}/{logs_path}{job_name}"\
                .format(bucket_name=self.s3_bucket,
                        logs_path=self.S3_GLUE_LOGS,
                        job_name=self.job_name)

            execution_role = self.get_iam_execution_role
            script_location = self._check_script_location()
            create_job_response = glue_client.create_job(
                Name=self.job_name,
                Description=self.desc,
                LogUri=s3_log_path,
                Role=execution_role['Role']['RoleName'],
                ExecutionProperty={"MaxConcurrentRuns": self.concurrent_run_limit},
                Command={"Name": "glueetl", "ScriptLocation": script_location},
                MaxRetries=self.retry_limit,
                AllocatedCapacity=self.num_of_dpus
            )
            # print(create_job_response)
            return create_job_response
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def _check_script_location(self):
        if self.script_location[:5] == self.S3_PROTOCOL:
            return self.script_location
        elif os.path.isfile(self.script_location):
            s3 = self.get_resource_type('s3', self.region_name)
            script_name = os.path.basename(self.script_location)
            s3.meta.client.upload_file(self.script_location,
                                       self.s3_bucket,
                                       self.S3_ARTIFACTS_PREFIX + script_name)

            s3_script_path = "s3://{s3_bucket}/{prefix}{job_name}/{script_name}" \
                .format(s3_bucket=self.s3_bucket,
                        prefix=self.S3_ARTIFACTS_PREFIX,
                        job_name=self.job_name,
                        script_name=script_name)
            return s3_script_path
        else:
            return None