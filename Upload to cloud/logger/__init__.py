import os
import sys
import logging
"""
Logger to log steps wherever it is required.
"""
logging_str = "[%(asctime)s] : %(levelname)s: %(module)s: %(message)s"
log_dir = "logs"
info_log_filepath = os.path.join(log_dir, "info_log.log")
error_log_filepath = os.path.join(log_dir, "error_log.log")
os.makedirs(log_dir, exist_ok=True)
# Create handlers
info_handler = logging.FileHandler(info_log_filepath)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter(logging_str))
error_handler = logging.FileHandler(error_log_filepath)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(logging_str))
# Create a stream handler to print logs to the terminal
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter(logging_str))
# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[info_handler, error_handler, stream_handler]
)
logger = logging.getLogger("api_loggerr")