from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional

import boto3

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


class DataUtils:
    def __init__(self, environment: Optional[str] = "dev"):
        self.environment = environment
        self.glue_client = boto3.client("glue")


    def read_data(
            self, 
            table_name: str,
            partition_values: Optional[Dict] = None
        ) -> DataFrame:
        """
        Função utilizada para ler uma tabela do Glue

        :param table_name (str): O nome da tabela a ser lida
        :param source_catalog (Optional(str)): O nome do catálogo de origem
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :return: O DataFrame
        """
        
        df = spark.table(f"{self.environment}_{table_name}")

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

        catalog = table_name.split(".")[0]
        spark.sql(f"USE {catalog}")

        writer = df.write.format("parquet")
        if partitions:
            writer = writer.partitionBy(partitions)
        if options:
            writer = writer.options(**options)

        writer.format("parquet")

        writer.saveAsTable(table_name, mode=mode)


    def copy_data(
            self, 
            table_name: str, 
            target_table_name: Optional[str], 
            months_of_data: Optional[str] = None
        ) -> None:
        """
        Função utilizada para copiar dados de uma tabela para outra

        :param table_name (str): O nome da tabela a ser copiada
        :param target_table_name (Optional[str]): O nome da tabela de destino
        :param months_of_data (Optional[str]): A quantidade de meses de dados a serem copiados
        
        """
        partition_columns = self.get_partition_columns(table_name)

        partition_values = {}
        if months_of_data and {"year", "month", "day"}.issubset(set(partition_columns)):
            date_from = datetime.now() - relativedelta(months=months_of_data)
            partition_values = {
                "year": str(date_from.year),
                "month": f"{date_from.month:02d}",
                "day": f"{date_from.day:02d}"
            }

        source_df = self.read_data(table_name=table_name, partition_values=partition_values)

        if not target_table_name:
            target_table_name = table_name

        self.write_data(source_df, table_name=target_table_name, partition=partition_columns, mode="overwrite")


    def create_sample_data(
            self, 
            table_name: str, 
            partition_values: Optional[Dict] = None, 
            percent: Optional[float] = 0.1
        ) -> None:
        """
        Função utilizada para criar uma amostra dos dados de uma tabela

        :param table_name (str): O nome da tabela a ser amostrada
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        """
        try:
            df = self.read_data(
                table_name=table_name,
                partition_values=partition_values
            )

            catalog_name = table_name.split(".")[0]
            spark.sql(f"USE {catalog_name}")
            
            (
                self.write_data(
                    df.sample(False, percent), 
                    table_name=f"{table_name}_sample",
                )
            )
        except AnalysisException as e:
            print(f"Error reading data: {e}")
    

    def get_partition_columns(self, table_name: str) -> List[str]:
        """
        Busca as colunas de partição de uma tabela

        :param table_name (str): O nome da tabela
        :return: A lista de colunas de partição
        """
        try:
            database_name = table_name.split(".")[0]
            table_name = table_name.split(".")[1]
            response = self.glue_client.get_table(DatabaseName=database_name, TableName=table_name)
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
