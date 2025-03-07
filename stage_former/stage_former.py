import sys
from pathlib import Path

from tables_info import TABLES_INFO, SERVICE_TABLES
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME
from logger import logger

class StageFormer:
    def __init__(self):
        self.db_handler = DBHandler()
    
    def create_tables(self):
        for table in TABLES_INFO:
            self.db_handler.create_table(schema_name=STAGE_SCHEMA_NAME,
                                            table_name=table["table_name"],
                                            columns=table["headers"])
            
    def create_service_tables(self):
        for table in SERVICE_TABLES:
            logger.log(f"{table["table_name"]} creation...")
            self.db_handler.execute_query(query = table["query"])



if __name__ == "__main__":
    sf = StageFormer()
    sf.db_handler.drop_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.db_handler.create_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.create_tables()
    sf.create_service_tables()
