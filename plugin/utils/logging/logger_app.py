import logging
import os
import pathlib


_log_format = "%(asctime)s - [%(levelname)s] - \
(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
_log_stream_format = "%(asctime)s - [%(levelname)s] - %(message)s"


def get_file_handler():
    file_handler = logging.FileHandler(os.path.join(pathlib.Path(__file__)
                                                    .parent.resolve(), "log", "plugin.log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_stream_format))
    return stream_handler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_stream_handler())
    return logger
