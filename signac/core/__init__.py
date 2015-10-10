import logging
logger = logging.getLogger(__name__)

try:
    import ssl  # test SSL availability
except ImportError:
    SSL_SUPPORT = False
else:
    SSL_SUPPORT = True
