import os
import logging
from logging.handlers import TimedRotatingFileHandler
from config import LOGGER_PATH

FORMAT = '[%(asctime)-15s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
loggers = {}

# Tạo thư mục LOGGER_PATH nếu chưa tồn tại
if not os.path.exists(LOGGER_PATH):
    os.makedirs(LOGGER_PATH)

def setup_logger(name, log_file, level=logging.DEBUG):
    """
    This function sets up a logger with the given name and log file.

    Parameters
    ----------
    name : str
        The name of the logger.
    log_file : str
        The path to the log file.
    level : int
        The logging level.

    Returns
    -------
    logger: The logger object.
    """
    if loggers.get(name):
        return loggers.get(name)

    formatter = logging.Formatter(FORMAT)
    handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=5, encoding='utf-8')
    handler.setFormatter(formatter)
    handler.setLevel(level)

    logger2 = logging.getLogger(name)
    logger2.setLevel(level)

    # Kiểm tra xem handler đã tồn tại chưa trước khi thêm
    if not any(isinstance(h, TimedRotatingFileHandler) for h in logger2.handlers):
        logger2.addHandler(handler)

    loggers[name] = logger2
    return logger2

def setup_logger_global(name, log_file, level=logging.DEBUG):
    """
    Setup a global logger with the given name and log file.

    Parameters:
        name (str): The name of the logger.
        log_file (str): The path to the log file.
        level (int, optional): The logging level. Defaults to logging.DEBUG.

    Returns:
        logger: The logger object.
    """
    # Sử dụng os.path.join để tạo đường dẫn log file chính xác
    return setup_logger(name, os.path.join(LOGGER_PATH, log_file), level)
