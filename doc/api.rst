.. _api:

=============
API Reference
=============

This is the API for the **signac** (core) application.

Command Line Interface
======================

Some core **signac** functions are---in addition to the Python interface---accessible
directly via the ``$ signac`` command.

For more information, please see ``$ signac --help``.

.. literalinclude:: cli-help.txt


The Project
===========

.. _python-api-project:

.. currentmodule:: signac

.. autoclass:: Project

.. rubric:: Attributes

.. autosummary::

    Project.build_job_search_index
    Project.build_job_statepoint_index
    Project.check
    Project.clone
    Project.config
    Project.create_access_module
    Project.create_linked_view
    Project.detect_schema
    Project.data
    Project.doc
    Project.document
    Project.dump_statepoints
    Project.export_to
    Project.find_job_documents
    Project.find_job_ids
    Project.find_jobs
    Project.find_statepoints
    Project.fn
    Project.get_id
    Project.get_statepoint
    Project.groupby
    Project.groupbydoc
    Project.import_from
    Project.index
    Project.isfile
    Project.min_len_unique_id
    Project.num_jobs
    Project.open_job
    Project.read_statepoints
    Project.repair
    Project.reset_statepoint
    Project.root_directory
    Project.sync
    Project.update_cache
    Project.update_statepoint
    Project.workspace
    Project.write_statepoints

.. autoclass:: Project
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: Job

The Job class
=============

.. _python-api-job:

.. currentmodule:: signac.contrib.job

.. autoclass:: Job

.. rubric:: Attributes

.. autosummary::

    Job.clear
    Job.close
    Job.data
    Job.doc
    Job.document
    Job.fn
    Job.get_id
    Job.init
    Job.isfile
    Job.move
    Job.open
    Job.remove
    Job.reset
    Job.reset_statepoint
    Job.sp
    Job.statepoint
    Job.sync
    Job.update_statepoint
    Job.workspace
    Job.ws

.. autoclass:: Job
    :members:
    :undoc-members:
    :show-inheritance:


The Collection
==============

.. currentmodule:: signac.contrib.collection

.. autoclass:: Collection
   :members:

Top-level functions
===================

.. automodule:: signac
    :members:
    :show-inheritance:
    :exclude-members: Project,Collection,RegexFileCrawler,MasterCrawler,SignacProjectCrawler


Submodules
==========

signac.cite module
------------------

.. automodule:: signac.cite
    :members:
    :undoc-members:
    :show-inheritance:

signac.sync module
------------------

.. automodule:: signac.sync
    :members:
    :undoc-members:
    :show-inheritance:

signac.warnings module
----------------------

.. automodule:: signac.warnings
    :members:
    :undoc-members:
    :show-inheritance:

signac.errors module
--------------------

.. automodule:: signac.errors
    :members:
    :undoc-members:
    :show-inheritance:
