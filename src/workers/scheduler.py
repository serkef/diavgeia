import datetime as dt
import signal
import sys
import time

import schedule

from config.diavgeia_config import DiavgeiaConfig
from utilities.logger_mixin import LoggerMixin
from workers.dispatcher import Dispatcher


class Scheduler(LoggerMixin):
    config: DiavgeiaConfig

    def __init__(self, config: DiavgeiaConfig):
        super().__init__(config)
        self.config = config
        self._shutdown_requested = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGTERM/SIGINT"""

        def signal_handler(signum, frame):
            signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
            self.log(f"Received {signal_name}. Requesting graceful shutdown...")
            self._shutdown_requested = True

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def start_daemon(self):
        self.log("Starting daemon mode scheduler...")
        schedule.every().day.at(
            self.config.schedule_time, self.config.schedule_timezone
        ).do(self._run_daily_job)

        self.log(
            f"Scheduled daily job at {self.config.schedule_time} {self.config.schedule_timezone}"
        )

        while not self._shutdown_requested:
            schedule.run_pending()
            time.sleep(60)

        self.log("Daemon shutdown requested. Exiting gracefully.")
        sys.exit(0)

    def _run_daily_job(self):
        self.log("Scheduler triggered daily job.")
        yesterday = (dt.datetime.now(dt.UTC) - dt.timedelta(days=1)).date()
        config = self.config.model_copy(update={"date_id": yesterday})
        try:
            Dispatcher(config=config).execute()
            self.log("Scheduler finished daily job successfully.")
        except Exception as e:
            self.logger.error(f"Daily job failed: {e}", exc_info=True)
            self.log("Daily job failed, but continuing daemon for next scheduled run")
