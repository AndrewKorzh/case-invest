import sys
from pathlib import Path


from tables_and_triggers import TABLES, FOREIGN_KEYS, TRIGGERS
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, ARCHIVE_PATH, STAGE_SCHEMA_NAME

class StageFormer:
    def __init__(self):
        self.db_handler = DBHandler()
    
    def create_tables(self):
        for table in TABLES:
            self.db_handler.execute_query(table)
    
    def create_foreign_keys(self):
        for fk in FOREIGN_KEYS:
            self.db_handler.execute_query(fk)

    def create_triggers(self):
        for tg in TRIGGERS:
            self.db_handler.execute_query(tg)


if __name__ == "__main__":
    sf = StageFormer()
    sf.db_handler.drop_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.db_handler.create_scheme(schema_name=STAGE_SCHEMA_NAME)
    sf.create_tables()
    sf.create_foreign_keys()
    sf.create_triggers()
