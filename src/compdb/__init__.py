import warnings

from signac import *

msg = "compdb was renamed to signac. Please import signac in the future."
warnings.warn(DeprecationWarning, msg)
