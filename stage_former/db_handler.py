import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

from logger import logger


# return f"alter column {column_name} type integer using coalesce({column_name}::text, null)::integer"
def alter_type_change(column_type:str, column_name):
    column_type = column_type.lower()
    if column_type == "integer":
        return f"alter column {column_name} type integer using nullif({column_name}::text, '')::integer"
    if column_type == "decimal(12,6)":
        return f"ALTER COLUMN {column_name}  TYPE DECIMAL(12,6) USING NULLIF({column_name}::TEXT, ''):: DECIMAL(12,6)"
    if column_type == "timestamp":
        return f"ALTER COLUMN {column_name} TYPE TIMESTAMP USING NULLIF({column_name}::TEXT, '')::TIMESTAMP"
    if column_type == "date":
        return f"ALTER COLUMN {column_name} TYPE DATE USING NULLIF({column_name}::TEXT, '')::DATE"
    return None

class DBHandler:
    def __init__(self):
        self.db_config = DB_CONFIG
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        self.connection = psycopg2.connect(**self.db_config)
        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        """Выполнение SQL-запроса"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise e

    def fetch_all(self, query, params=None):
        """Выборка всех данных"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception as e:
                raise e
            
    def clear_table(self, schema_name, table_name):
        query = f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY CASCADE;"
        self.execute_query(query)
            
    def copy_data(self,
                table_name,
                schema_name,
                file_path,
                headers,
                has_headers):
        if self.cursor:
            try:
                with open(file_path, 'r') as f:
                    self.cursor.copy_expert(
                    sql=f"""
                        COPY {schema_name}.{table_name} ({",".join(headers)})
                        FROM STDIN WITH (FORMAT CSV, HEADER {has_headers}, DELIMITER ',')
                        """,
                        file=f
                    )
                    self.connection.commit()
            except Exception as e:
                raise e
       

    def create_table_text(self, schema_name, table_name, columns):
        query = f"""
            create table {schema_name}.{table_name}
            (
            {",".join([f"{c} text" for c in columns])}
            );
            """
        self.execute_query(query=query)


    def bad_source(self, 
                    schema_name,
                    bad_source_table_name,
                    table_name,
                    source: str,
                    length: int
                   ):
        
        query = f"""
            INSERT INTO {schema_name}.{bad_source_table_name} 
            (table_name, source, length)
            VALUES 
            ('{table_name}', '{source}', {length});
        """
        self.execute_query(query=query)
    
    def process_unique(self,
                    schema_name,
                    table_name,
                    table_primary_key,
                    column_name,
                    error_table
                    ):
        query_create_temp_table = f"""
        create temp table temp_{table_name}_errors as
        with cte as (
            select 
                {column_name},
                row_number() over (partition by {column_name} order by {column_name}) as row_num,
                now() as error_ddtm
            from {schema_name}.{table_name}
        )
        select * from cte
        where row_num > 1;
        """
        query_insert_error = f"""
        insert into {schema_name}.{error_table} 
        (table_name, id, error_type, error_message, error_dttm)
        select 
            '{table_name}' as table_name,
            {table_primary_key}::integer as id,
            'not_unique' as error_type,
            'Duplicate in {column_name}' as error_message, -- Добавляем значение
            error_ddtm
        from temp_{table_name}_errors;
        """
        query_delete_duplicates = f"""
        DELETE FROM {schema_name}.{table_name}
        WHERE {column_name} IN (
            SELECT {column_name}
            FROM temp_{table_name}_errors
        );
        """
        query_drop_temp_table = f"""
        drop table if exists temp_{table_name}_errors;
        """

        try:
            self.execute_query(query_drop_temp_table)
            self.execute_query(query_create_temp_table)
            self.execute_query(query_insert_error)
            self.execute_query(query_delete_duplicates)
        finally:
            self.execute_query(query_drop_temp_table)

    def change_table_types(self,
                     schema_name,
                     table_name,
                     headers: dict):
        alters = []
        for key, val in headers.items():
            alter = alter_type_change(column_type=val["type"],column_name=key)
            if alter:
                alters.append(alter)
        query = f"""
            alter table {schema_name}.{table_name}
            {",\n".join(alters)}
        """
        self.execute_query(query=query)
    

    def check_data_loss(self, schema_name, table_name):
        q = f"""
        WITH actual_data AS (
            SELECT 
                COUNT(*) AS loaded_data
            FROM {schema_name}.{table_name}
        ),
        bad_source_loss AS (
            SELECT 
                COALESCE(SUM(length), 0) AS bad_source_loss
            FROM {schema_name}.bad_source
            WHERE table_name = '{table_name}'
        ),
        error_log_loss AS (
            SELECT 
                COUNT(*) AS error_log_loss
            FROM {schema_name}.error_log
            WHERE table_name = '{table_name}'
        )
        SELECT 
            --'{table_name}' AS table_name,
            COALESCE(ad.loaded_data, 0) AS loaded_data,
            COALESCE(bs.bad_source_loss, 0) + COALESCE(el.error_log_loss, 0) AS data_loss
        FROM actual_data ad
        LEFT JOIN bad_source_loss bs ON true
        LEFT JOIN error_log_loss el ON true;
        """
        data_loaded, data_los = self.fetch_all(q)[0]

        return (data_loaded, data_los)




    def create_scheme(self, schema_name):
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        logger.log(f"{schema_name} created")


    def drop_scheme(self, schema_name):
        self.execute_query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        logger.log(f"{schema_name} droped")
        

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        logger.log("connection closed")
    

if __name__ == "__main__":
    dh = DBHandler()
    logger.log(dh.fetch_all(f"select* from test_tables2.product_type"))
