import logging
logger = logging.getLogger(__name__)

from . import contrib
from . import core
from . import db

def check_mpi_support_or_raise():
    logger.debug("Checking MPI support.")
    try:
        import mpi4py
    except ImportError as error:
        msg = "Unable to determine MPI rank. Missing `mpi4py` package."
        raise EnvironmentError(msg)

def raise_no_mpi4py_error():
    msg = "No MPI support. Install package `mpi4py` for MPI support."
    raise EnvironmentError(msg)
