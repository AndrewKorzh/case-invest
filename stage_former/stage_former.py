import sys
from pathlib import Path

from .stage_tables_info import TABLES_INFO, SERVICE_TABLES
from .stage_db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME
from logger import logger, LogLevel

class StageFormer:
    def __init__(self):
        self.db_handler = DBHandler()
    
    def create_tables(self):
        for table in TABLES_INFO:
            logger.log(f"{table["table_name"]} creation...", level=LogLevel.INFO)
            self.db_handler.create_table(schema_name=STAGE_SCHEMA_NAME,
                                            table_name=table["table_name"],
                                            columns=table["headers"])
            
    def create_service_tables(self):
        for table in SERVICE_TABLES:
            logger.log(f"{table["table_name"]} creation...", level=LogLevel.INFO)
            self.db_handler.execute_query(query = table["query"])

    def delete_tables(self, names):
        for name in names:
            logger.log(f"delete {name}...", level=LogLevel.INFO)
            self.db_handler.delete_table(schema_name=STAGE_SCHEMA_NAME, table_name=name)

    def run(self):
        # sf.db_handler.drop_scheme(schema_name=STAGE_SCHEMA_NAME)
        tables_names = [t["table_name"] for t in TABLES_INFO]
        service_tables_names = [t["table_name"] for t in SERVICE_TABLES]
        self.delete_tables(tables_names)
        self.delete_tables(service_tables_names)
        self.db_handler.create_scheme(schema_name=STAGE_SCHEMA_NAME)
        self.create_tables()
        self.create_service_tables()
        

if __name__ == "__main__":
    sf = StageFormer()
    sf.run()




