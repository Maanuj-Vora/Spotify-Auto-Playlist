import logging
import os


class Logger:
    def __init__(
        self, name="spotify_playlist_sync", log_level=logging.INFO, log_file=None
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
            )

            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

            if log_file:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                fh = logging.FileHandler(log_file)
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)

    def get_logger(self):
        return self.logger
