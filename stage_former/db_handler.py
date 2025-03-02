class DBHandler:
    def __init__(self):
        return
    

if __name__ == "__main__":

    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    import config
    print(config.db_config)
