from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Run data quality checks on one or more tables.
    
    Parameters:
    data_check_query: A list of one or more queries to check data.
    table: A list of one or more tables for the data check queries.
    expected_results: A list of expected results for each data check query.
    
    Returns: Exception raised on data check failure.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_check_query=[],
                 table=[],
                 expected_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.data_check_query=data_check_query
        self.table=table
        self.expected_result=expected_result

    def execute(self, context):
        self.log.info('Running data quality checks')
        
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        checks = zip(self.data_check_query, self.table, self.expected_result)
        for check in checks:
            try:
                redshift.run(check[0].format(check[1])) == check[2]
                self.log.info('Data quality check passed.')
            except:
                self.log.info('Data quality check failed.')
                raise AssertionError('Data quality check failed.')