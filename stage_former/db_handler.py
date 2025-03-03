import psycopg2

class DBHandler:
    def __init__(self, db_config):
        self.db_config = db_config
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
        print("copy_data...")
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
                    print("data copied...")
                    self.connection.commit()
            except Exception as e:
                raise e
            
        print(self.fetch_all(f"select* from {schema_name}.{table_name}"))
        print(f"{schema_name}.{table_name}")
       
# column1, column2, column3

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        print("connection closed")
    

if __name__ == "__main__":

    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    import config
    print(config.db_config)

    dh = DBHandler(config.db_config)
    print(dh.fetch_all(f"select* from test_tables.temp_table"))
