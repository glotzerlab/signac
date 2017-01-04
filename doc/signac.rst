.. _api:

API
===

The complete **signac** application interface (API).

Command Line Interface
----------------------

Some core **signac** functions are -- in addition to the Python interface -- accessible
directly via the ``$ signac`` command.

For more information, please see ``$ signac --help``.

.. code-block:: bash

    $ signac --help
    usage: signac [-h] [--debug] [--version] [-y]
                  {init,project,job,statepoint,move,clone,index,find,view,config}
                  ...

    signac aids in the management, access and analysis of large-scale
    computational investigations.

    positional arguments:
      {init,project,job,statepoint,move,clone,index,find,view,config}

    optional arguments:
      -h, --help            show this help message and exit
      --debug               Show traceback on error for debugging.
      --version             Display the version number and exit.
      -y, --yes             Answer all questions with yes. Useful for scripted
                            interaction.

Module contents
---------------

.. automodule:: signac
    :members:
    :undoc-members:
    :show-inheritance:


.. automodule:: signac.cite
    :members:

Subpackages
-----------

.. toctree::

    signac.contrib
    signac.db
    signac.core
    signac.common
