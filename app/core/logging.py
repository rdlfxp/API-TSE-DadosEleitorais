import logging


def get_api_logger() -> logging.Logger:
    logger = logging.getLogger("api.meucandidato")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    return logger


logger = get_api_logger()
