""" Utilities module """

import logging
import os
from pathlib import Path

import aiofiles
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
LOG_DIR = Path(os.environ["LOG_DIR"])
EXPORT_DIR = Path(os.environ["EXPORT_DIR"])


def get_logger(logger_name: str, log_file: str):
    """ Method to return a custom logger with the given name and level """

    logger = logging.getLogger(logger_name)

    # Set Level
    logger.setLevel(os.getenv("LOGLEVEL", "INFO"))

    # Set Format
    fmt = (
        "%(asctime)s - %(name)s - %(levelname)s -"
        " %(funcName)s:%(lineno)d - %(message)s"
    )
    formatter = logging.Formatter(fmt)

    # Creating stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Creating and adding the file handler
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_DIR / log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


async def async_save_file(content: bytes, filepath: Path):
    """ gets a byte stream and saves it to the filepath in async """

    async with aiofiles.open(filepath, "wb") as out:
        await out.write(content)
        await out.flush()
