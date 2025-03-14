import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

from logger import logger, LogLevel


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
        self.cursor.execute(query, params)
        self.connection.commit()
        return True


    # def execute_query(self, query, params=None):
    #     try:
    #         self.cursor.execute(query, params)
    #         self.connection.commit()
    #     except Exception as e:
    #         # self.connection.rollback()
    #         logger.log(f"Ошибка выполнения SQL: {e}")
    #         return False
    #     return True
    

    def fetch_all(self, query, params=None):
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception as e:
                raise e
            
    def clear_table(self, schema_name, table_name):
        query = f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY CASCADE;"
        self.execute_query(query)

    def count_lines_amount(self, schema_name, table_name):
        query = f"select count(*) from {schema_name}.{table_name}"
        result = self.fetch_all(query)
        if result:
            return result[0][0]
        return 0


    def coppy_data_attempt(self, table_name, schema_name, file_path, columns, has_headers, null_exp):
        sql = f"""
            COPY {schema_name}.{table_name} ({",".join(columns)}) 
            FROM STDIN WITH (FORMAT CSV, HEADER {has_headers}, DELIMITER ',', QUOTE '''', NULL '{null_exp}')
        """
        if self.cursor:
            try:
                with open(file_path, 'r') as f:
                    self.cursor.copy_expert(sql=sql, file=f)
                self.connection.commit()
                return True
            except Exception as e:
                self.connection.rollback()
            return False
            
    def copy_data(self, table_name, schema_name, file_path, columns, has_headers):
        if self.coppy_data_attempt(table_name, schema_name, file_path, columns, has_headers, null_exp ='""'):
            return True
        if self.coppy_data_attempt(table_name, schema_name, file_path, columns, has_headers, null_exp =''):
            return True
        logger.log(f"\nОшибка в {table_name} при загрузке дынных\n", level=LogLevel.ERROR)
        return False


    def create_table(self, schema_name, table_name, columns:dict):
        query = f"""
            create table {schema_name}.{table_name}
            (
            {",".join([f"{column_name} {column_type}" for column_name, column_type in columns.items()])}
            );
            """
        self.execute_query(query=query)


    def add_bad_source(self, 
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

    def add_fatal_error(self, 
                    schema_name,
                    fatal_error_table_name,
                    message,
                   ):
        
        query = f"""
            INSERT INTO {schema_name}.{fatal_error_table_name} 
            (message)
            VALUES 
            ('{message}');
        """
        self.execute_query(query=query)

    def insert_row_count_comparison(self, schema_name, row_count_comparison_table_name, table_name, source_length, db_table_length):
        self.execute_query(f"""
                INSERT INTO {schema_name}.{row_count_comparison_table_name} 
                (table_name, source_length, db_table_length)
                VALUES ('{table_name}', '{source_length}', {db_table_length});""")

    def process_unique(self,
                    schema_name,
                    table_name,
                    table_primary_key,
                    column_name,
                    error_type,
                    error_table
                    ):
        
        q = f"""
            INSERT INTO {schema_name}.{error_table} (table_name, id, error_type, error_message)
            SELECT 
                '{table_name}',
                t.{table_primary_key},
                '{error_type}',
                'Duplicate entry found'
            FROM {schema_name}.{table_name} t
            JOIN (
                SELECT {table_primary_key}
                FROM {schema_name}.{table_name}
                GROUP BY {column_name}
                HAVING COUNT(*) > 1
            ) d ON t.{table_primary_key} = d.{table_primary_key};
            """
        self.execute_query(q)

    def bad_source_count(self,schema_name, bad_source_table_name):
        q = f"""
            select count(*) from {schema_name}.{bad_source_table_name} bs
        """
        res = self.fetch_all(q)
        return res[0][0]
    
    def row_count_comparison(self, schema_name, row_count_comparison_table_name):
        q = f"""
        select rcc.table_name,
        rcc.source_length,
        rcc.db_table_length 
                from {schema_name}.{row_count_comparison_table_name} rcc 
        """
        res = self.fetch_all(q)
        return res
    
    def error_count(self, schema_name, error_log_table_name):
        q = f"""
        select el.table_name, count(*) from {schema_name}.{error_log_table_name} el
        group by el.table_name
        """
        res = self.fetch_all(q)
        return res




    def process_bound_value(self,
                    schema_name,
                    table_name,
                    table_primary_key,
                    column_name,
                    error_type,
                    value,
                    sign,
                    error_table
                    ):
        
        q = f"""
                INSERT INTO {schema_name}.{error_table} (table_name, id, error_type, error_message)
                SELECT 
                    '{table_name}',
                    t.{table_primary_key},
                    '{error_type}',
                    '{column_name} {sign} {value}'
                FROM {schema_name}.{table_name} t
                WHERE t.{column_name} {sign} {value};
            """
        self.execute_query(q)
    
    def delete_table(self, schema_name, table_name):
        query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;"
        self.execute_query(query)

    def create_scheme(self, schema_name):
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        logger.log(f"{schema_name} created", level=LogLevel.INFO)


    def drop_scheme(self, schema_name):
        self.execute_query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        logger.log(f"{schema_name} droped", level=LogLevel.INFO)
        
    def copy_table(self, schema_from, schema_to, table_name):
        drop_table_query = f"""
        DROP TABLE IF EXISTS {schema_to}.{table_name};
        """
        
        create_table_query = f"""
        CREATE TABLE {schema_to}.{table_name} (
            LIKE {schema_from}.{table_name} INCLUDING ALL
        );
        """
        
        insert_data_query = f"""
        INSERT INTO {schema_to}.{table_name}
        SELECT * FROM {schema_from}.{table_name};
        """
        self.execute_query(drop_table_query)
        self.execute_query(create_table_query)
        self.execute_query(insert_data_query)


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        logger.log("connection closed", level=LogLevel.INFO)
    

if __name__ == "__main__":
    dh = DBHandler()
    logger.log(dh.fetch_all(f"select* from test_tables2.product_type"), level=LogLevel.INFO)
