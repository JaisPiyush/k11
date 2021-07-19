import logging


logging.basicConfig(
    format="%(asctime)s--%(levelname)s : %(message)s"
)


def log(msg, level=0):
    logging.log(level, msg)