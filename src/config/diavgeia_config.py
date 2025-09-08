import datetime as dt
from pathlib import Path

from pydantic_settings import BaseSettings


class DiavgeiaConfig(BaseSettings):
    # API Credentials (required)
    diavgeia_api_user: str
    diavgeia_api_password: str

    # Worker settings
    download_workers: int
    download_pdf: bool

    # Directory settings
    log_path: Path
    export_path: Path

    # Date settings
    date_id: dt.date

    # Logging settings
    log_level: str

    # Debug settings
    limit: Optional[int] = None  # Limit number of documents to fetch for debugging

    model_config = {
        "env_file": Path(__file__).parent.parent.parent / ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "forbid",
    }
