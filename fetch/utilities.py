""" Utilities module """

import json
import logging
import os
from pathlib import Path
from typing import Dict

import aiofiles
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
LOG_DIR = Path(os.environ["LOG_DIR"])


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
    file_handler = logging.FileHandler(LOG_DIR / log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


async def save_json(json_obj: Dict, filepath: Path):
    """ Gets a json obj and saves it to filepath """

    async with aiofiles.open(filepath, "w") as out:
        await out.write(json.dumps(json_obj, ensure_ascii=False, sort_keys=True))
        await out.flush()
