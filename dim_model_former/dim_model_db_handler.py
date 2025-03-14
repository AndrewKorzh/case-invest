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
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            logger.log(f"\nОшибка выполнения SQL: {e}\n", level=LogLevel.ERROR)
            return False
        return True


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

    def delete_table(self, schema_name, table_name):
        query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;"
        self.execute_query(query)

    def count_lines_amount(self, schema_name, table_name):
        query = f"select count(*) from {schema_name}.{table_name}"
        result = self.fetch_all(query)
        if result:
            return result[0][0]
        return 0
    
    def add_last_data_update_date(self, schema_name, table_name):
        q = f"""
            INSERT INTO {schema_name}.{table_name} (success, date)
            VALUES (True, CURRENT_DATE);
            """
        self.execute_query(q)


    def create_scheme(self, schema_name):
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        logger.log(f"{schema_name} created", level=LogLevel.INFO)


    def drop_scheme(self, schema_name):
        self.execute_query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        logger.log(f"{schema_name} droped", level=LogLevel.INFO)
        

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
    logger.log(dh.fetch_all(f"select* from test_tables2.product_type"), level=LogLevel.DEBUG)


