import logging
import sys

def setup_logging(logger_name: str = None) -> logging.Logger:
    """
    Configures and returns a logger instance. mainly for debugging in docker 
    use `docker logs dashboard_project-api-1` to see the output on the host machine
    """
    
    # Define log format
    log_format = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Configure root logger to output to stdout
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True
    )

    logging.getLogger("smbprotocol").setLevel(logging.WARNING)

    for uvicorn_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(uvicorn_logger_name)
        uv_logger.handlers = []  # Clear Uvicorn's default formatting handlers
        uv_logger.propagate = True  # Propagate messages to the configured root logger
    return logging.getLogger(logger_name)

