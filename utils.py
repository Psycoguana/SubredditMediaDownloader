import asyncio
import logging

import requests
from aiohttp import client_exceptions


def get_logger(name, logger_level=logging.INFO):
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s,%(msecs)d -> %(filename)s:%(lineno)d [%(levelname)s] -> %(message)s')

    # Log to sysout.
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Log to a file.
    file_handler = logging.FileHandler('logs.log', 'a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.setLevel(logger_level)

    return logger


def retry_connection(func):
    logger = get_logger(__name__, logging.INFO)

    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except (client_exceptions.ClientError,
                requests.exceptions.ConnectionError,
                asyncio.exceptions.TimeoutError) as error:
            logger.error(f"\nError trying to connect. {type(error).__name__}: {error}")
            sleep_secs = 10
            for i in range(sleep_secs):
                logger.info(f"\rRetrying in {sleep_secs - i}...", end='', flush=True)

            return await wrapper(*args, **kwargs)

        except Exception as error:
            logger.error(f"\nError downloading post:")
            logger.error(f"\t{type(error).__name__}: {error}")
            raise

    return wrapper
