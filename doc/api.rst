.. _api:

=============
API Reference
=============

This is the API for the **signac** (core) application.

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
    Project.find_job_ids
    Project.find_jobs
    Project.fn
    Project.get_id
    Project.get_statepoint
    Project.groupby
    Project.groupbydoc
    Project.import_from
    Project.id
    Project.index
    Project.isfile
    Project.min_len_unique_id
    Project.num_jobs
    Project.open_job
    Project.read_statepoints
    Project.repair
    Project.reset_statepoint
    Project.root_directory
    Project.stores
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
    Job.id
    Job.init
    Job.isfile
    Job.move
    Job.open
    Job.remove
    Job.reset
    Job.reset_statepoint
    Job.sp
    Job.statepoint
    Job.stores
    Job.sync
    Job.update_statepoint
    Job.workspace
    Job.ws

.. autoclass:: Job
    :members:
    :undoc-members:
    :show-inheritance:


.. currentmodule:: signac

The Collection
==============

.. autoclass:: Collection
   :members:


The JSONDict
============

This class implements the interface for the job's :attr:`~signac.contrib.job.Job.statepoint` and :attr:`~signac.contrib.job.Job.document` attributes, but can also be used stand-alone:

.. autoclass:: JSONDict
   :members:


The H5Store
===========

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.data` attribute, but can also be used stand-alone:

.. autoclass:: H5Store
    :members:


The H5StoreManager
==================

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.stores` attribute, but can also be used stand-alone:

.. autoclass:: H5StoreManager
    :members:
    :show-inheritance:


Top-level functions
===================

.. automodule:: signac
    :members:
    :show-inheritance:
    :exclude-members: Project,Collection,RegexFileCrawler,MainCrawler,MasterCrawler,SignacProjectCrawler,JSONDict,H5Store,H5StoreManager


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
