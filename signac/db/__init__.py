import warnings
try:
    import pymongo  # noqa
except ImportError:
    warnings.warn("Failed to import pymongo. "
                  "get_database will not be available.", ImportWarning)
    __all__ = []
else:
    from .database import get_database
    __all__ = ['get_database']
