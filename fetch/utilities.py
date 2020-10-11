""" Utilities module """
import hashlib
import logging
import os
from pathlib import Path
from typing import Union

import aiofiles
import urllib3
from dotenv import find_dotenv, load_dotenv

# Basic Configuration
load_dotenv(find_dotenv())
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
DIAVGEIA_API_USER = os.environ["DIAVGEIA_API_USER"]
DIAVGEIA_API_PASSWORD = os.environ["DIAVGEIA_API_PASSWORD"]
DOWNLOAD_PDF = os.environ["DOWNLOAD_PDF"] == "True"
BUCKET_NAME = os.environ["BUCKET_NAME"]
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_KEY = os.environ["B2_KEY"]
B2_UPLOAD_PATH = os.environ["B2_UPLOAD_PATH"]
ASYNC_WORKERS = int(os.environ["ASYNC_WORKERS"])
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


async def md5(filepath: Union[str, Path]) -> str:
    """ Checks the the md5 of a file with asynchronous file read """

    checksum = hashlib.md5()
    async with aiofiles.open(filepath, mode="rb") as f_in:
        checksum.update(await f_in.read())
    return checksum.hexdigest()
