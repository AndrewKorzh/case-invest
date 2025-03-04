import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

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
        q = f"""
            create table {schema_name}.{table_name}
            (
            {",".join([f"{c} text" for c in columns])}
            );
            """
        self.execute_query(query=q)


    def create_scheme(self, schema_name):
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        print(f"{schema_name} created")


    def drop_scheme(self, schema_name):
        self.execute_query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        print(f"{schema_name} droped")
        

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        print("connection closed")
    

if __name__ == "__main__":
    dh = DBHandler()
    print(dh.fetch_all(f"select* from test_tables2.product_type"))
