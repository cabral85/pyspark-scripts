from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional

import boto3

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


class DataProcessor:
    def __init__(self, environment: Optional[str] = "dev"):
        self.environment = environment
        self.bucket = "my-bucket"
        self.glue_client = boto3.client("glue")

        spark.sql(f"USE {environment}")


    def read_data(
            self,
            table_name: str,
            source_catalog: Optional[str] = None,
            partition_values: Optional[Dict] = None
        ) -> DataFrame:
        """
        Função utilizada para ler uma tabela do Glue

        :param table_name (str): O nome da tabela a ser lida
        :param source_catalog (Optional(str)): O nome do catálogo de origem
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :return: O DataFrame
        """

        if not source_catalog:
            source_catalog = self.environment
        
        df = spark.table(f"{source_catalog}.{table_name}")

        if partition_values:
            partition_filters = [f"{key} = '{value}'" for key, value in partition_values.items()]
            combined_filter = " AND ".join(partition_filters)
            df = df.filter(combined_filter)

        return df


    def write_data(
            self, 
            df: DataFrame,
            table_name: str, 
            partitions: Optional[List[str]] = None, 
            options: Optional[Dict] = None, 
            mode: Optional[str] ="append"
        ) -> None:
        """
        Função utilizada para escrever um DataFrame em uma tabela do Glue

        :param df (DataFrame): O DataFrame a ser escrito
        :param table_name (str): O nome da tabela a ser escrita
        :param partitions (Optional[List[str]]): As partições a serem usadas
        :param options (Optional[Dict]): As opções a serem usadas
        """

        writer = df.write.format("parquet")
        if partitions:
            writer = writer.partitionBy(partitions)
        if options:
            writer = writer.options(**options)

        writer.format("parquet")

        writer.saveAsTable(table_name, mode=mode)


    def copy_data(
            self, 
            source_table_name: str, 
            source_catalog: str,
            target_table_name: str, 
            months_of_data: Optional[str] = None,
            mode: Optional[str] = "append"
        ) -> None:
        """
        Função utilizada para copiar dados de uma tabela para outra

        :param source_table_name (str): O nome da tabela a ser copiada
        :param source_catalog (str): O nome do catálogo de origem
        :param target_table_name (str): O nome da tabela de destino
        :param months_of_data (Optional[str]): A quantidade de meses de dados a serem copiados
        
        """
        partition_columns = self.get_partition_columns(source_catalog, source_table_name)

        partition_values = {}
        if months_of_data and {"year", "month", "day"}.issubset(set(partition_columns)):
            date_from = datetime.now() - relativedelta(months=months_of_data)
            partition_values = {
                "year": str(date_from.year),
                "month": f"{date_from.month:02d}",
                "day": f"{date_from.day:02d}"
            }

        source_df = self.read_data(table_name=source_table_name, partition_values=partition_values)

        self.write_data(source_df, table_name=target_table_name, partition=partition_columns, mode=mode)


    def create_sample_data(
            self, 
            table_name: str, 
            source_catalog: str,
            sufix: Optional[str] = "_sample",
            partition_values: Optional[Dict] = None, 
            percent: Optional[float] = 0.1
        ) -> None:
        """
        Função utilizada para criar uma amostra dos dados de uma tabela

        :param table_name (str): O nome da tabela a ser amostrada
        :param source_catalog (str): O nome do catálogo de origem
        :param sufix (Optional[str]): O sufixo a ser adicionado ao nome da tabela
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :param percent (Optional[float]): A porcentagem de amostragem
        """
        try:
            df = self.read_data(
                table_name=table_name,
                source_catalog=source_catalog,
                partition_values=partition_values
            )
            
            self.write_data(df.sample(False, percent),table_name=f"{table_name}{sufix}"
            )
        except AnalysisException as e:
            print(f"Error reading data: {e}")


    def create_glue_catalog(self, catalog_name: str) -> None:
        """
        Função utilizada para criar um catálogo no Glue

        :param catalog_name (str): O nome do catálogo
        """
        try:
            location = f"s3://{self.bucket}/{self.environment}/{catalog_name}"
            self.glue_client.create_database(DatabaseInput={"Name": catalog_name, "LocationUri": location})
        except Exception as e:
            print(f"Error creating catalog: {e}")
    

    def get_partition_columns(self, catalog_name: str, table_name: str) -> List[str]:
        """
        Busca as colunas de partição de uma tabela

        :param catalog_name (str): O nome do catálogo
        :param table_name (str): O nome da tabela
        :return: A lista de colunas de partição
        """
        try:
            response = self.glue_client.get_table(DatabaseName=catalog_name, TableName=table_name)
            partition_keys = response["Table"]["PartitionKeys"]
            return [key["Name"] for key in partition_keys]
        except Exception as e:
            print(f"Error retrieving partition columns: {e}")
            return []


    def get_glue_catalogs(self):
        """
        Busca a lista de catálogos do Glue

        :return: A lista de catálogos
        """
        response = self.glue_client.get_databases()
        return [db["Name"] for db in response["DatabaseList"]]
    

    def get_tables_in_catalog(self, catalog_name: str):
        """
        Busca a lista de tabelas em um catálogo do Glue

        :param catalog_name (str): O nome do catálogo
        :return: A lista de tabelas
        """
        response = self.glue_client.get_tables(DatabaseName=catalog_name)
        return [table["Name"] for table in response["TableList"]]
