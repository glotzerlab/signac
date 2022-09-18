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

    Project.check
    Project.clone
    Project.config
    Project.create_linked_view
    Project.detect_schema
    Project.data
    Project.doc
    Project.document
    Project.export_to
    Project.find_jobs
    Project.fn
    Project.groupby
    Project.import_from
    Project.isfile
    Project.min_len_unique_id
    Project.open_job
    Project.path
    Project.repair
    Project.stores
    Project.sync
    Project.update_cache
    Project.workspace

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
    Job.id
    Job.init
    Job.isfile
    Job.move
    Job.open
    Job.path
    Job.project
    Job.remove
    Job.reset
    Job.sp
    Job.statepoint
    Job.stores
    Job.sync
    Job.update_statepoint

.. autoclass:: Job
    :members:
    :undoc-members:
    :show-inheritance:


.. currentmodule:: signac

The JSONDict
============

This class implements the interface for the job's :attr:`~signac.contrib.job.Job.statepoint` and :attr:`~signac.contrib.job.Job.document` attributes, but can also be used on its own.

.. autoclass:: JSONDict
    :members:
    :inherited-members:


The H5Store
===========

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.data` attribute, but can also be used on its own.

.. autoclass:: H5Store
    :members:
    :inherited-members:


The H5StoreManager
==================

This class implements the interface to the job's :attr:`~signac.contrib.job.Job.stores` attribute, but can also be used on its own.

.. autoclass:: H5StoreManager
    :members:
    :inherited-members:
    :show-inheritance:


Top-level functions
===================

.. automodule:: signac
    :members:
    :show-inheritance:
    :exclude-members: Project,JSONDict,H5Store,H5StoreManager


Submodules
==========

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
