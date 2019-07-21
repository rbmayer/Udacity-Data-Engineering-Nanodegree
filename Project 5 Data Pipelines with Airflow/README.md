# DEND Project 5: Data Pipelines with Airflow

## Project Summary

A fictional music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines. They have come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in Amazon S3 buckets and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets may consist of CSV or JSON logs that record user activity in the application and store metadata about the songs that have been played.

For this project, I have created a high grade data pipeline using the Airflow python API. The pipeline is dynamic, built from reusable tasks, can be monitored, allows easy backfills, and conducts automated data quality checks.

This project was submitted for Udacity's Data Engineering Nanodegree (DEND) in July 2019.

## How to Run

Prerequisites: Access to AWS credentials and an Amazon Redshift cluster.

1. Put project files in their respective folders in an Airflow installation.
2. Adjust parameters in the DAG script, udac_example_dag.py, as desired.
3. Create aws_credentials and redshift connections in Airflow.
3. Launch udac_example_dag from the Airflow UI.

## Files in Repository

```/airflow/dags/udac_example_dag.py``` DAG definition file. Calls  Operators to stage data to redshift, populate the data warehouse and run data quality checks.

```/airflow/create_tables.sql``` Optional script to create staging and data warehouse tables in Redshift.

```/airflow/plugins/operators/stage_redshift.py``` Defines the custom operator **StageToRedshiftOperator**. This operator loads data from S3 to staging tables in redshift. User may specify csv or JSON file format. Csv file options include delimiter and whether to ignore headers. JSON options include automatic parsing or use of JSONpaths file in the COPY command.

```/airflow/plugins/operators/load_fact.py``` Defines the custom operator **LoadFactOperator**. This operator appends data from staging tables into the main fact table.

```/airflow/plugins/operators/load_dimension.py``` Defines the custom operator **LoadDimensionOperator**. This operator loads data into dimension tables from staging tables. Update mode can set to 'insert' or 'overwrite'.

```/airflow/plugins/operators/data_quality.py``` Defines the custom operator **DataQualityOperator**. This operator performs any number of data quality checks at the end of the pipeline run. The project provides two pre-defined checks in the helper files:
  * empty_table_check: raises task error on finding 0 rows in user-specified table
  * songplay_id_check: raises task error when a duplicate primary key (songplay_id) is detected in the fact table  

Users can specify their own data quality checks by entering the SQL query, table name and expected results as parameters.
