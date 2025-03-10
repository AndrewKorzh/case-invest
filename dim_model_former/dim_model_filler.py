import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from dim_model_db_handler import DBHandler
from dim_model_tables_info import DIM_MODEL_SCRIPTS

sys.path.append(str(Path(__file__).parent.parent))
from config import ARCHIVE_PATH, STAGE_SCHEMA_NAME
from logger import logger


class DimModelFiller:
    def __init__(self):
        self.db_handler = DBHandler()

    def fill_tables(self):
        for s in DIM_MODEL_SCRIPTS:
            self.db_handler.execute_query(s)


if __name__ == "__main__":
    dmf = DimModelFiller()
    dmf.fill_tables()
