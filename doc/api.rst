.. _api:

=============
API Reference
=============

This is the API for the **signac** (core) application.

The Project
===========

.. _python-api-project:

.. currentmodule:: signac

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


The JobsCursor class
====================
.. _python-api-jobscursor:

.. currentmodule:: signac.contrib.project

.. rubric:: Attributes

.. autosummary::
    JobsCursor.export_to
    JobsCursor.groupby
    JobsCursor.groupbydoc
    JobsCursor.to_dataframe


.. autoclass:: JobsCursor
    :members:
    :undoc-members:
    :show-inheritance:


The Job class
=============

.. _python-api-job:

.. currentmodule:: signac.contrib.job

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
    :inherited-members:


The H5Store
===========

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.data` attribute, but can also be used stand-alone:

.. autoclass:: H5Store
    :members:
    :inherited-members:


The H5StoreManager
==================

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.stores` attribute, but can also be used stand-alone:

.. autoclass:: H5StoreManager
    :members:
    :inherited-members:
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

synced\_collections package
===========================

Submodules
----------

synced\_collections.synced\_collection module
-----------------------------------------------

.. automodule:: signac.core.synced_collections.synced_collection
   :members:
   :undoc-members:
   :private-members:
   :show-inheritance:

synced\_collections.buffered\_collection module
-----------------------------------------------

.. automodule:: signac.core.synced_collections.buffered_collection
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.file\_buffered\_collection module
-----------------------------------------------

.. automodule:: signac.core.synced_collections.file_buffered_collection
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.serialized\_file\_buffered\_collection module
-----------------------------------------------

.. automodule:: signac.core.synced_collections.serialized_file_buffered_collection
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.memory\_buffered\_collection module
-----------------------------------------------

.. automodule:: signac.core.synced_collections.memory_buffered_collection
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.caching module
----------------------------------

.. automodule:: signac.core.synced_collections.caching
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.collection\_json module
-------------------------------------------

.. automodule:: signac.core.synced_collections.collection_json
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.collection\_mongodb module
----------------------------------------------

.. automodule:: signac.core.synced_collections.collection_mongodb
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.collection\_redis module
--------------------------------------------

.. automodule:: signac.core.synced_collections.collection_redis
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.collection\_zarr module
-------------------------------------------

.. automodule:: signac.core.synced_collections.collection_zarr
   :members:
   :undoc-members:
   :show-inheritance:

synced\_collections.utils module
-------------------------------------------

.. automodule:: signac.core.synced_collections.utils
   :members:
   :undoc-members:
   :show-inheritance:
