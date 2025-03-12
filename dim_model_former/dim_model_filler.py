import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from .dim_model_db_handler import DBHandler
from .dim_model_tables_info import DIM_MODEL_SCRIPTS, LAST_DATA_UPDATE_DATE

sys.path.append(str(Path(__file__).parent.parent))
from config import DIM_MODEL_SCHEMA_NAME
from logger import logger, LogLevel


class DimModelFiller:
    def __init__(self):
        self.db_handler = DBHandler()

    def fill_tables(self):
        for key, val in DIM_MODEL_SCRIPTS.items():
            logger.log(f"filling {key}...", level=LogLevel.INFO)
            start_time = time.perf_counter()
            self.db_handler.execute_query(val)
            elapsed_time = time.perf_counter() - start_time
            logger.log(f"Время: {elapsed_time:.2f} секунд", level=LogLevel.INFO)

    def run(self):
        self.fill_tables()
        self.db_handler.add_last_data_update_date(schema_name=DIM_MODEL_SCHEMA_NAME, table_name=LAST_DATA_UPDATE_DATE)


if __name__ == "__main__":
    dmf = DimModelFiller()
    dmf.run()
