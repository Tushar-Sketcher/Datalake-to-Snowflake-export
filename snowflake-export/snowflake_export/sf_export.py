import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('__name__')

SNOWFLAKE_FORMAT = 'net.snowflake.spark.snowflake'


class SnowflakeConnector:
    def __init__(self, user, password, account, warehouse, database, schema, role):
        self.connection: SnowflakeConnection = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )

    def cursor(self) -> SnowflakeCursor:
        return self.connection.cursor()


def get_snowflake_ddl(df: DataFrame, table_name: str) -> str:
    db_to_sf_dtype_map = {
        'int': 'int',
        'bigint': 'bigint',
        'float': 'float',
        'double': 'double',
        'boolean': 'boolean',
        'string': 'varchar',
        'timestamp': 'timestamp'
    }
    query = f'create or replace table {table_name} ('
    ddl_elements = []
    for col_name, col_type in df.dtypes:
        if col_type in db_to_sf_dtype_map:
            ddl_elements.append(f'{col_name} ' + db_to_sf_dtype_map[col_type])
        else:
            # use `variant` as a catch-all for other types
            ddl_elements.append(f'{col_name} variant')

    query += ', '.join(ddl_elements).strip() + ')'
    return query


def ingest_to_snowflake(
        input_db_table: str,
        snowflake_table: str,
        incremental_flag: bool,
        snowflake_final_table: str,
        incremental_cols: list[str],
        snowflake_url: str,
        snowflake_account: str,
        snowflake_user: str,
        snowflake_password: str,
        snowflake_database: str,
        snowflake_schema: str,
        snowflake_warehouse: str,
        snowflake_role: str
) -> None:
    spark = SparkSession.builder.appName("Databricks to Snowflake").enableHiveSupport().getOrCreate()

    # get the schema from the input hive table
    sf_options = {
        'sfURL': snowflake_url,
        'sfUser': snowflake_user,
        'sfPassword': snowflake_password,
        'sfDatabase': snowflake_database,
        'sfSchema': snowflake_schema,
        'sfWarehouse': snowflake_warehouse,
        'sfRole': snowflake_role
    }

    query = f'select * from {input_db_table}'

    input_df = spark.sql(query)

    table_ddl = get_snowflake_ddl(input_df, snowflake_table)

    snowflake_connector = SnowflakeConnector(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        role=snowflake_role
    )

    cursor: SnowflakeCursor = snowflake_connector.cursor()

    # create or replace table in Snowflake
    result_set = cursor.execute(table_ddl)

    if result_set is not None:
        logger.info(f'Successfully created table {snowflake_table}')
    else:
        raise Exception(f'Error executing Snowflake query: {table_ddl}')

    # write data to the newly created table
    input_df.write.format(SNOWFLAKE_FORMAT).options(**sf_options).option('dbtable', snowflake_table).mode(
        'overwrite').save()

    logger.info(f'Successfully persisted data into Snowflake table {snowflake_table}')

    if incremental_flag == True:
        delete_query = f"delete from {snowflake_final_table} where {' and '.join([f'{col} in (select distinct {col} from {snowflake_table})' for col in incremental_cols])}"

        cursor.execute(delete_query)

        insert_query = f"insert into {snowflake_final_table} select * from {snowflake_table}"

        cursor.execute(insert_query)
        logger.info(f'Successfully inserted data into Snowflake table {snowflake_final_table}')

        drop_query = f"drop table {snowflake_schema}.{snowflake_table}"
        cursor.execute(drop_query)
        logger.info(f'{snowflake_table} droped Successfully.')
