import datetime as dt
import logging

from config.diavgeia_config import DiavgeiaConfig


class LoggerMixin:
    _log_filename = f"diavgeia.{dt.datetime.now().isoformat()}.log"
    config: DiavgeiaConfig
    logger: logging.Logger

    def __init__(self, config: DiavgeiaConfig):
        self.config = config
        logger_name = f"DiavgeiaDailyFetch.{self.__class__.__name__}.{config.date_id}"
        self.logger = self.get_logger(logger_name, self._log_filename)

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
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # file handler
        self.config.log_path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(self.config.log_path / log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger
