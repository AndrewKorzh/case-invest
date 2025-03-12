class LogLevel:
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5

import sys

LOGGER_MIN_LEVEL = LogLevel.INFO

class Logger:
    def __init__(self):
        pass

    def log(self, message, level=LogLevel.INFO, r033=False, end="\n"):
        if level < LOGGER_MIN_LEVEL:
            return

        if r033:
            sys.stdout.write("\r\033[K")
            end = ""

        sys.stdout.write(f"{message}{end}")
        sys.stdout.flush()

logger = Logger()
