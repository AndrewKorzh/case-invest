import sys
from pathlib import Path

from dim_model_db_handler import DBHandler
from dim_model_tables_info import DIM_MODEL_TABLES

sys.path.append(str(Path(__file__).parent.parent))

from config import DIM_MODEL_SCHEMA_NAME
from logger import logger

class DimModelFormer:
    def __init__(self):
        self.db_handler = DBHandler()
       
    def create_tables(self):
        for table in DIM_MODEL_TABLES:
            logger.log(f"{table["table_name"]} creation...")
            self.db_handler.execute_query(query = table["query"])



if __name__ == "__main__":
    sf = DimModelFormer()
    sf.db_handler.drop_scheme(schema_name=DIM_MODEL_SCHEMA_NAME)
    sf.db_handler.create_scheme(schema_name=DIM_MODEL_SCHEMA_NAME)
    sf.create_tables()