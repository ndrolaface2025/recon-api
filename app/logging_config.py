import logging, sys
from pythonjsonlogger import jsonlogger
def configure_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]
