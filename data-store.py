from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Union

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

import boto3


class DataProcessor:
    def __init__(self, environment: str):
        self.environment = environment
        self.glue_client = boto3.client('glue')


    """
    Read data from a given path

    :param table_name: The name of the table to read
    :param s3_path: The path to read from
    :param options: The options to use when reading
    :param partition_values: The partition values to filter on
    :return: The DataFrame
    """
    def read_data(self, table_name: str = None, s3_path: str = None, options: str = None, partition_values: str = None):
        if table_name:
            df = spark.table(table_name)
        elif s3_path:
            reader = spark.read
            if options:
                for key, value in options.items():
                    reader.option(key, value)
            df = reader.load(s3_path)

        if partition_values:
            partition_filters = [f"{partition} = '{value}'" for partition, value in partition_values.items()]
            combined_filter = " AND ".join(partition_filters)
            df = df.filter(combined_filter)

        return df


    """
    Write data to a given path

    :param df: The DataFrame to write
    :param path: The path to write to
    :param table_name: The name of the table to write to
    :param partition: The partition columns
    :param options: The options to use when writing
    """
    def write_data(self, df, path, table_name, partition=None, options=None, mode='append'):
        try:
            # Check if the table already exists
            spark.sql(f"DESCRIBE TABLE {table_name}")
            table_exists = True
        except AnalysisException:
            # Table does not exist
            table_exists = False

        writer = df.write.format('parquet')
        if partition:
            writer = writer.partitionBy(partition)
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)

        if table_exists:
            # If the table exists, overwrite the data at the specified path
            writer.mode(mode).save(path=path)
        else:
            # If the table does not exist, save as a new table
            writer.saveAsTable(table_name, mode=mode, path=path)


    """
    Copy data from one environment to another

    :param source_env: The source environment
    :param target_env: The target environment
    :param table_name: The name of the table to copy
    :param target_table_name: The name of the table to copy to
    :param target_path: The path to copy to
    :param months_of_data: The number of months of data to copy
    """
    def copy_data(self, source_env: str, table_name: str, target_table_name: str = None, target_path: str = None, months_of_data: str = None):
        partition_columns = self.get_partition_columns(source_env, table_name)

        partition_values = {}
        if months_of_data and {'year', 'month', 'day'}.issubset(set(partition_columns)):
            date_from = datetime.now() - relativedelta(months=months_of_data)
            partition_values = {
                'year': str(date_from.year),
                'month': f"{date_from.month:02d}",
                'day': f"{date_from.day:02d}"
            }

        source_df = self.read_data(table_name=table_name, partition_values=partition_values)

        if not target_table_name:
            target_table_name = table_name

        self.write_data(source_df, target_path, target_table_name, partition_columns)
    

    """
    Get the partition columns for a given table in a given database

    :param database_name: The name of the database
    :param table_name: The name of the table
    :return: A list of partition columns
    """
    def get_partition_columns(self, database_name, table_name):
        try:
            response = self.glue_client.get_table(DatabaseName=database_name, TableName=table_name)
            partition_keys = response['Table']['PartitionKeys']
            return [key['Name'] for key in partition_keys]
        except Exception as e:
            print(f"Error retrieving partition columns: {e}")
            return []


    """
    Get the list of Glue catalogs

    :return: A list of Glue catalogs
    """
    def get_glue_catalogs(self):
        response = self.glue_client.get_databases()
        return [db['Name'] for db in response['DatabaseList']]
    

    """
    Get the list of tables in a given Glue catalog

    :param catalog_name: The name of the Glue catalog
    :return: A list of tables
    """
    def get_tables_in_catalog(self, catalog_name):
        response = self.glue_client.get_tables(DatabaseName=catalog_name)
        return [table['Name'] for table in response['TableList']]
