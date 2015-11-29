import warnings
try:
    import PySide  # noqa
except ImportError:
    warnings.warn("Failed to import PySide. "
                  "gui will not be available.", ImportWarning)

    def main():
        """Start signac-gui.

        The gui is only available if PySide is installed."""
        raise ImportError(
            "You need to install PySide to use the gui.")
else:
    from .gui import main

__all__ = ['main']
