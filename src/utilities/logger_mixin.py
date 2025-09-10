import datetime as dt
import logging
from logging.handlers import RotatingFileHandler

from config.diavgeia_config import DiavgeiaConfig


class LoggerMixin:
    _log_filename = f"diavgeia.{dt.datetime.now().isoformat()}.log"
    config: DiavgeiaConfig
    logger: logging.Logger

    def __init__(self, config: DiavgeiaConfig):
        self.config = config
        self.logger = self.get_logger(self.logger_name, self._log_filename)

    @property
    def logger_name(self) -> str:
        if self.__class__.__name__ == "Scheduler":
            return f"Diavgeia.Scheduler"
        if self.__class__.__name__ == "Crawler" and hasattr(self, "worker_id"):
            return f"Diavgeia.{self.worker_id}"
        if self.__class__.__name__ == "Dispatcher" and self.config.start_date:
            return f"Diavgeia.Dispatcher-{self.config.start_date.strftime('%Y%m%d')}"
        return f"Diavgeia.{self.__class__.__name__}"

    def log(self, msg: str):
        self.logger.info(msg)

    def warn(self, msg: str):
        self.logger.warning(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)

    def get_logger(self, logger_name: str, log_file: str) -> logging.Logger:
        """Method to return a custom logger with the given name and level"""
        logger = logging.getLogger(logger_name)
        # Avoid adding multiple handlers to the same logger
        if logger.handlers:
            return logger

        logger.setLevel(self.config.log_level)
        fmt = (
            "%(asctime)s - %(name)s - %(levelname)s -"
            " %(funcName)s:%(lineno)d - %(message)s"
        )
        formatter = logging.Formatter(fmt)

        # stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(self.config.log_level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # file handler with rotation - 50MB per file, keep 5 backups
        self.config.log_path.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            self.config.log_path / log_file,
            maxBytes=50 * 1024 * 1024,
            backupCount=5,
        )
        file_handler.setLevel(self.config.log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger
