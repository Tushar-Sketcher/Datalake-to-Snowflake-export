import argparse
from snowflake_export.sf_export import ingest_to_snowflake


def ingest_to_snowflake_entry():
    parser = argparse.ArgumentParser(description='Exporting data to snowflake')
    parser.add_argument('--snowflake_final_table', type=str, help='final table name for snowflake', required=True)
    parser.add_argument('--snowflake_table_name', type=str, help='The temp table name to be used for incremental load')
    parser.add_argument('--snowflake_database', type=str,
                        help='snowflake db', required=True)
    parser.add_argument('--delta_table_to_be_exported', type=str, help='Table name which is to be exported', required=True)
    parser.add_argument('--snowflake_warehouse', type=str, help='Snowflake warehouse name where data is to be exported', required=True)
    parser.add_argument('--snowflake_account', type=str, help='Snowflake account', required=True)
    parser.add_argument('--snowflake_role', type=str, help='Snowflake account', required=True)
    parser.add_argument('--snowflake_schema', type=str, help='Snowflake schema', required=True)
    parser.add_argument('--snowflake_url', type=str, help='Snowflake URL', required=True)
    parser.add_argument('--snowflake_user', type=str, help='Snowflake user', required=True)
    parser.add_argument('--snowflake_password', type=str, help='Snowflake password', required=True)
    parser.add_argument('--incremental_flag', type=str, help='incremental flag', required=True)
    parser.add_argument('--incremental_col', type=str, help='column name to update data')
    args = parser.parse_args()

    ingest_to_snowflake(
    input_db_table=args.delta_table_to_be_exported,
    snowflake_table=args.snowflake_table_name,
    incremental_flag= args.incremental_flag,
    snowflake_final_table= args.snowflake_final_table,
    incremental_col=args.incremental_col,
    snowflake_url=args.snowflake_url,
    snowflake_account=args.snowflake_account,
    snowflake_user=args.snowflake_user,
    snowflake_password=args.snowflake_password,
    snowflake_database=args.snowflake_database,
    snowflake_schema=args.snowflake_schema,
    snowflake_warehouse=args.snowflake_warehouse,
    snowflake_role=args.snowflake_role
)




    


