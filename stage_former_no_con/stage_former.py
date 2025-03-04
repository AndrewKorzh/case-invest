import sys
from pathlib import Path

from tables_info import TABLES_INFO
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import STAGE_SCHEMA_NAME

class StageFormer:
    def __init__(self):
        self.db_handler = DBHandler()
    
    def create_tables(self):
        for table in TABLES_INFO:
            self.db_handler.create_table_text(schema_name=STAGE_SCHEMA_NAME,
                                            table_name=table["table_name"],
                                            columns=list(table["headers"].keys()))

if __name__ == "__main__":
    sf = StageFormer()
    sf.db_handler.drop_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.db_handler.create_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.create_tables()
