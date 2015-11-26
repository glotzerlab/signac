import warnings
try:
    import pymongo  # noqa
except ImportError:
    warnings.warn("Failed to import pymongo. "
                  "get_database will not be available.", ImportWarning)

    def get_database(*args, **kwargs):
        """Get a database handle.

        This function is only available if pymongo is installed."""
        raise ImportError(
            "You need to install pymongo to use `get_database()`.")
else:
    from .database import get_database


__all__ = ['get_database']
