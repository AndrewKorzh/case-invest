import sys
from pathlib import Path

from stage_tables_info import TABLES_INFO, SERVICE_TABLES
from stage_db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME
from logger import logger

class StageFormer:
    def __init__(self):
        self.db_handler = DBHandler()
    
    def create_tables(self):
        for table in TABLES_INFO:
            logger.log(f"{table["table_name"]} creation...")
            self.db_handler.create_table(schema_name=STAGE_SCHEMA_NAME,
                                            table_name=table["table_name"],
                                            columns=table["headers"])
            
    def create_service_tables(self):
        for table in SERVICE_TABLES:
            logger.log(f"{table["table_name"]} creation...")
            self.db_handler.execute_query(query = table["query"])

    def delete_tables(self, names):
        for name in names:
            print(f"delete {name}...")
            self.db_handler.delete_table(schema_name=STAGE_SCHEMA_NAME, table_name=name)



if __name__ == "__main__":
    sf = StageFormer()
    # sf.db_handler.drop_scheme(schema_name=STAGE_SCHEMA_NAME)


    tables_names = [t["table_name"] for t in TABLES_INFO]
    service_tables_names = [t["table_name"] for t in SERVICE_TABLES]
    sf.delete_tables(tables_names)
    sf.delete_tables(service_tables_names)

    sf.db_handler.create_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.create_tables()
    sf.create_service_tables()
