import logging
import os
import sys

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_TO_FILE = bool(os.getenv("LOG_TO_FILE", False))
FILE_LOG_NAME = os.getenv("FILE_LOG_NAME", "app.log")

def get_logger(logger_name: str = None) -> logging.Logger:
    """
    Configures and returns a logger instance. mainly for debugging in docker
    use `docker logs dashboard_project-api-1` to see the output on the host machine
    Verbosity is controlled by the LOG_LEVEL env var (e.g. DEBUG, INFO, WARNING).
    Defaults to INFO, so debug messages are hidden unless LOG_LEVEL=DEBUG is set.
    """

    log_format = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"


    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(LOG_LEVEL)

    handlers = [stream_handler]

    
    if LOG_TO_FILE:
            file_handler = logging.FileHandler(FILE_LOG_NAME)
            file_handler.setLevel(LOG_LEVEL)
            handlers.append(file_handler)

    # Root logger level MUST be set no higher than the handler levels so messages aren't discarded globally
    logging.basicConfig(
        level=LOG_LEVEL,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True
    )

    logging.getLogger("smbprotocol").setLevel(logging.WARNING)

    for uvicorn_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(uvicorn_logger_name)
        uv_logger.handlers = []  # Clear Uvicorn's default formatting handlers
        uv_logger.propagate = True  # Propagate messages to the configured root logger
        
    return logging.getLogger(logger_name)