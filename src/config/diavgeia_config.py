import datetime as dt
from pathlib import Path

from pydantic_settings import BaseSettings


class DiavgeiaConfig(BaseSettings):
    # API Credentials (required)
    diavgeia_api_user: str
    diavgeia_api_password: str

    # Worker settings
    crawl_workers: int
    download_workers: int
    download_pdf: bool

    # Directory settings
    log_path: Path
    export_path: Path

    # Date settings
    date_id: dt.date

    # Logging settings
    log_level: str

    model_config = {
        "env_file": Path(__file__).parent.parent.parent / ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "forbid",
    }
