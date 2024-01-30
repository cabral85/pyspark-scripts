from pyspark.sql import SparkSession
import boto3
from datetime import datetime, timedelta

class DataProcessor:
    def __init__(self, environment):
        self.environment = environment
        self.spark = SparkSession.builder.appName("DataProcessor").getOrCreate()
        self.glue_client = boto3.client('glue')

    def read_data(self, table_name=None, s3_path=None, options=None, partitions=None, days=None):
        if table_name:
            df = self.spark.table(table_name)
        elif s3_path:
            reader = self.spark.read
            if options:
                for key, value in options.items():
                    reader.option(key, value)
            df = reader.load(s3_path)

        if partitions and days is not None:
            cutoff_date = datetime.now() - timedelta(days=days)
            for partition in partitions:
                df = df.filter(df[partition] >= cutoff_date.strftime('%Y-%m-%d'))

        return df

    def write_data(self, df, path, table_name, partition=None, options=None):
        writer = df.write.format('parquet')
        if partition:
            writer.partitionBy(partition)
        if options:
            for key, value in options.items():
                writer.option(key, value)
        writer.save(path=path, mode='overwrite')
        # If you want to save it as a table as well
        df.write.saveAsTable(table_name, format='parquet', mode='overwrite', path=path)

    def copy_data(self, source_env, target_env, table_name, partitions=None, target_table_name=None, target_path=None):
        # Assuming that the environment dictates the path or table to read from
        source_df = self.read_data(table_name=table_name, partitions=partitions)

        if not target_table_name:
            target_table_name = table_name

        self.write_data(source_df, path=target_path, table_name=target_table_name, partition=partitions)

    def get_glue_catalogs(self):
        response = self.glue_client.get_databases()
        return [db['Name'] for db in response['DatabaseList']]

    def get_tables_in_catalog(self, catalog_name):
        response = self.glue_client.get_tables(DatabaseName=catalog_name)
        return [table['Name'] for table in response['TableList']]

# Example usage
processor = DataProcessor("test")
df = processor.read_data(table_name="your_table", options={'inferSchema': 'true', 'header': 'true'})
processor.write_data(df, path="s3://your-bucket/path", table_name="target_table", partition="year")
processor.copy_data("test_env", "prod_env", "your_table", partitions=["year", "month"], target_path="s3://your-target-bucket/path")
