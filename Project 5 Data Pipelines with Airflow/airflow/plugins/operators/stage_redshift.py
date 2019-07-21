from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Transfer data from S3 to staging tables in redshift database.
    
    Parameters:
    aws_credentials_id: Conn Id of the Airflow connection to Amazon Web Services
    redshift_conn_id: Conn Id of the Airflow connection to redshift database
    table: name of the staging table to populate
    s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    s3_key: name of S3 key. This field is templatable when context is enabled, e.g. "log_data/{execution_date.year}/{execution_date.month}/"
    delimiter: csv field delimiter
    ignore_headers: '0' or '1'
    data_format: 'csv' or 'json'
    jsonpaths: path to JSONpaths file
    
    Returns: None
    """
    template_fields = ("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 data_format="csv",
                 jsonpaths="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket # udacity-dend
        self.s3_key=s3_key # log_data, song_data
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.data_format=data_format.lower()     # 'csv', 'json'
        self.jsonpaths=jsonpaths


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        
        # select csv or json format
        if self.data_format == 'csv':
            autoformat = "DELIMITER '{}'".format(self.delimiter)
        elif self.data_format == 'json':
            json_option = self.jsonpaths or 'auto'
            autoformat = "FORMAT AS JSON '{}'".format(json_option)
        
        # set S3 path based on execution dates  
        rendered_key = self.s3_key.format(**context)
        self.log.info('Rendered key is ' + rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            autoformat
        )
        redshift.run(formatted_sql)





