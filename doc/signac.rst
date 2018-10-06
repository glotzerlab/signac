.. _api:

===
API
===

The complete **signac** application programming interface (API).

Command Line Interface
======================

Some core **signac** functions are---in addition to the Python interface---accessible
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


Python API
==========

.. _python-api-project:

Project
-------
.. currentmodule:: signac

.. autoclass:: Project

.. rubric:: Attributes

.. autosummary::

    ~Project.build_job_search_index
    ~Project.build_job_statepoint_index
    ~Project.check
    ~Project.clone
    ~Project.config
    ~Project.create_access_module
    ~Project.create_linked_view
    ~Project.detect_schema
    ~Project.doc
    ~Project.document
    ~Project.dump_statepoints
    ~Project.export_to
    ~Project.find_job_documents
    ~Project.find_job_ids
    ~Project.find_jobs
    ~Project.find_statepoints
    ~Project.fn
    ~Project.get_id
    ~Project.get_statepoint
    ~Project.groupby
    ~Project.groupbydoc
    ~Project.import_from
    ~Project.index
    ~Project.isfile
    ~Project.min_len_unique_id
    ~Project.num_jobs
    ~Project.open_job
    ~Project.read_statepoints
    ~Project.repair
    ~Project.reset_statepoint
    ~Project.root_directory
    ~Project.sync
    ~Project.update_cache
    ~Project.update_statepoint
    ~Project.workspace
    ~Project.write_statepoints


.. autoclass:: Project
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: Job

.. _python-api-job:

Job
---
.. currentmodule:: signac.contrib.job

.. autoclass:: Job

.. rubric:: Attributes

.. autosummary::

    ~Job.clear
    ~Job.close
    ~Job.doc
    ~Job.document
    ~Job.fn
    ~Job.get_id
    ~Job.init
    ~Job.isfile
    ~Job.move
    ~Job.open
    ~Job.remove
    ~Job.reset
    ~Job.reset_statepoint
    ~Job.sp
    ~Job.statepoint
    ~Job.sync
    ~Job.update_statepoint
    ~Job.workspace
    ~Job.ws


.. autoclass:: signac.contrib.job.Job
    :members:
    :undoc-members:
    :show-inheritance:


Signac
------

.. automodule:: signac
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: Project


.. automodule:: signac.cite
    :members:


Subpackages
===========

.. toctree::

    signac.contrib
    signac.db
    signac.core
    signac.common
