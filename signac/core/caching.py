import uuid
import logging

logger = logging.getLogger(__name__)


def get_cache():
    try:
        import redis
        CACHE = redis.Redis()
        test_key = str(uuid.uuid4())
        CACHE.set(test_key, 0)
        assert CACHE.get(test_key) == b'0'  # redis store data as bytes
        CACHE.delete(test_key)
    except Exception as error:
        logger.debug(str(error))
        logger.info("Redis not available, using per-instance cache.")
        CACHE = dict()
    else:
        logger.info("Using redis cache.")
    return CACHE
