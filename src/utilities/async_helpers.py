import hashlib
from pathlib import Path
from typing import Union

import aiofiles


async def save_file(content: bytes, filepath: Path):
    """gets a byte stream and saves it to the filepath in async"""
    async with aiofiles.open(filepath, "wb") as out:
        await out.write(content)
        await out.flush()


async def md5(filepath: Union[str, Path]) -> str:
    """Checks the md5 of a file with asynchronous file read"""
    checksum = hashlib.md5()
    async with aiofiles.open(filepath, mode="rb") as f_in:
        checksum.update(await f_in.read())
    return checksum.hexdigest()
