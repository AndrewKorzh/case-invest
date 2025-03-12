import sys
from pathlib import Path

from .dim_model_db_handler import DBHandler
from .dim_model_tables_info import DIM_MODEL_TABLES, LAST_DATA_UPDATE_DATE_QUERY

sys.path.append(str(Path(__file__).parent.parent))

from config import DIM_MODEL_SCHEMA_NAME
from logger import logger, LogLevel

class DimModelFormer:
    def __init__(self):
        self.db_handler = DBHandler()
       
    def create_tables(self):
        for key, val in DIM_MODEL_TABLES.items():
            logger.log(f"{key} creation...",  level=LogLevel.INFO)
            self.db_handler.execute_query(query = val)
        self.db_handler.execute_query(query = LAST_DATA_UPDATE_DATE_QUERY)
        

    def delete_tables(self, names):
        for name in names:
            logger.log(f"delete {name}",  level=LogLevel.INFO)
            self.db_handler.delete_table(schema_name=DIM_MODEL_SCHEMA_NAME, table_name=name)
        return
    
    def run(self):
        self.delete_tables(names=list(DIM_MODEL_TABLES.keys()))
        self.db_handler.create_scheme(schema_name=DIM_MODEL_SCHEMA_NAME)
        self.create_tables()
        return



if __name__ == "__main__":
    sf = DimModelFormer()
    sf.run()
