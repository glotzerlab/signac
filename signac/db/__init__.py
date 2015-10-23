import warnings
try:
    from .database import get_database
except ImportError:
    warnings.warn("Failed to import pymongo. "
                  "get_database will not be available.", ImportWarning)
    __all__ = []
else:
    __all__ = ['get_database']
