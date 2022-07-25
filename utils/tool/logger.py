import os
import platform
import logging
from utils.tool.configer import Config


def log(filename, log_name):
    conf = Config()
    log_path = os.path.join(conf.path, os.sep.join(['logs', f"{log_name}.txt"]))
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)
    try:
        filing = logging.FileHandler(filename=log_path)
    except FileNotFoundError:
        if platform.system().lower() == "windows":
            os.mkdir(r"\\".join(log_path.split('\\')[:-1]))
        else:
            os.mkdir("/".join(log_path.split('/')[:-1]))
        filing = logging.FileHandler(filename=log_path)
    else:
        pass
    streaming = logging.StreamHandler()
    formatter = logging.Formatter(fmt="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    filing.setFormatter(formatter)
    streaming.setFormatter(formatter)
    logger.addHandler(filing)
    logger.addHandler(streaming)
    return logger


