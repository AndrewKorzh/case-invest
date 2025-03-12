import time

class LogLevel:
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5

import sys

LOGGER_MIN_LEVEL = LogLevel.DEBUG

class Logger:
    def __init__(self):
        pass

    def log(self, message, level=LogLevel.INFO, r033=False, end="\n"):
        if level < LOGGER_MIN_LEVEL:
            return

        if r033:
            sys.stdout.write("\r\033[K")
            end = ""

        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        sys.stdout.write(f"[{current_time}] {message}{end}")
        sys.stdout.flush()

logger = Logger()
