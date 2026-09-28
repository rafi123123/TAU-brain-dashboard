import logging
import sys

def get_logger(logger_name: str = None, log_file: str = "app.log") -> logging.Logger:
    """
    Configures and returns a logger instance. mainly for debugging in docker 
    use `docker logs dashboard_project-api-1` to see the output on the host machine
    """
    
    log_format = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Root logger level MUST be set to INFO (or DEBUG) so INFO messages aren't discarded globally
    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        datefmt=date_format,
        handlers=[stream_handler, file_handler],
        force=True
    )

    logging.getLogger("smbprotocol").setLevel(logging.WARNING)

    for uvicorn_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(uvicorn_logger_name)
        uv_logger.handlers = []  # Clear Uvicorn's default formatting handlers
        uv_logger.propagate = True  # Propagate messages to the configured root logger
        
    return logging.getLogger(logger_name)