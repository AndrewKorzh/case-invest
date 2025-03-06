import sys


class Logger:
    def __init__(self):
        pass

    def log(self, message, r033 = False, end = "\n"):
        if r033:
            sys.stdout.write("\r\033[K")
        sys.stdout.write(f"{message}{end}")
        sys.stdout.flush()

logger = Logger()


