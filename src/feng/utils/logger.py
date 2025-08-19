import logging
import time
import os

class Log:
    def __init__(self, logger=None):
        # Create a logger with the specified name
        self.name = logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # Avoid adding handlers multiple times
        if not self.logger.handlers:  # Check if handlers already exist
            # Create a handler for logging to a file
            self.log_time = time.strftime("%Y%m%d")
            file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../log")

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            self.log_path = os.path.join(file_dir, f"{self.name}_{self.log_time}.log")

            file_handler = logging.FileHandler(self.log_path, "a", encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)

            # Create a handler for logging to the console
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Define a formatter with function name and line number
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            # Disable propagation to prevent logging duplication
            self.logger.propagate = False

    def getlog(self):
        """Returns the configured logger."""
        return self.logger

    def getpath(self):
        """Returns the log file path."""
        return self.log_path