import warnings

from signac import *
__all__ = ['core', 'contrib', 'db']

msg = "compdb was renamed to signac. Please import signac in the future."
print('Warning!',msg)
warnings.warn(msg, DeprecationWarning)
