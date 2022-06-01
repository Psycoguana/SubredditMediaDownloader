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


def _get_post_id(args, kwargs):
    if 'name' in kwargs:
        return kwargs['name'].split('.')[0]
    else:
        return args[1].id


def retry_connection(func):
    logger = get_logger(__name__, logging.INFO)

    async def wrapper(*args, **kwargs):
        post_id = _get_post_id(args, kwargs)

        for tries in range(1, 6):
            try:
                return await func(*args, **kwargs)
            except (client_exceptions.ClientError,
                    requests.exceptions.ConnectionError,
                    asyncio.exceptions.TimeoutError) as error:

                logger.debug(f"Error trying to connect. {type(error).__name__}: {error}")

                if tries < 5:
                    logger.debug(f"Try {tries}/5. Retrying in 10 seconds...")
                else:
                    # If we got to this point, we've tried 5 times without succeeding.
                    logger.error(f"Too many retries. Post will be skipped: {post_id}")
                    return None

            except Exception as error:
                logger.error(f"\nError downloading post: {post_id}")
                logger.error(f"\t{type(error).__name__}: {error}")

                return None

    return wrapper
