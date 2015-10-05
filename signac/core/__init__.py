import logging
logger = logging.getLogger(__name__)

try:
    import ssl
except ImportError:
    SSL_SUPPORT = False
else:
    SSL_SUPPORT = True
