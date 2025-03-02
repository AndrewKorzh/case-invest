import os
import glob
import sys
from pathlib import Path

from data_structure import DATA_STRUCTURE
# print(DATA_STRUCTURE)


class StageFormer:
    # data_path
    def __init__(self, ):
        pass

    def get_all_dir_files(self, directory_path, extention = "csv"):
        file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
        return file_paths




if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent))
    import config

    sf = StageFormer()
    archive_path = config.ARCHIVE_PATH
    full_path = os.path.join(archive_path, "crm_transaction")

    print(sf.get_all_dir_files(full_path))

