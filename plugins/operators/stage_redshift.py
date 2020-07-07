from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Executes a COPY command to load files from s3 to Redshift staging tables 
    
    Inspired in this operator (https://airflow.readthedocs.io/en/stable/_modules/airflow/operators/s3_to_redshift_operator.html)

    """
    
    ui_color = '#358140'

    copy_query = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults)
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 redshift_conn_id = "",
                 aws_credential_id = "",
                 log_file = "",
                 file_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_file = log_file
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_credential_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f" Extract table {self.table} from : {s3_path}")    
        
        if self.log_file != "":
            self.log_file = "s3://{}/{}".format(self.s3_bucket, self.log_file)
            copy_query = self.copy_query.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.log_file)
        else:
            copy_query = self.copy_query.format(self.table, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        
        self.log.info(f"Executing copy query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table} staged with success")




