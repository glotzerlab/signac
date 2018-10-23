.. _quickreference:

===============
Quick Reference
===============

Projects
========

A **signac** :py:class:`.Project` is the primary data interface for the generation and management of project data spaces.

Start a new project
-------------------

Python:

.. code-block:: python

   project = signac.init_project('MyProject')

Command line:

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject
    Initialized project 'MyProject'.

Access the project
------------------

Python:

.. code-block:: python

    # Within or below the project root directory:
    project = signac.get_project()

    # With explicit path specification:
    project = signac.get_project(root='/path/to/project')

Command line:

.. code-block:: bash

    $ signac project
    MyProject

Open a job
----------

Python:

.. code-block:: python

    # Open with state point
    with project.open_job({'a': 0}) as job:
        pass

     # Open with job id
     with project.open_job(id='9bfd29df07674bc4aa960cf661b5acd2') as job:
        pass

     # Open with abbreviated job id
     with project.open_job(id='9bfd29') as job:
        pass

     # From a job workspace directory
     with signac.get_job() as job:
        pass

Command line:

.. code-block:: bash

    $ signac job '{"a": 0}'
    9bfd29df07674bc4aa960cf661b5acd2

    $ signac job --workspace '{"a": 0}'
    /path/to/workspace/9bfd29df07674bc4aa960cf661b5acd2

    $ signac statepoint 9bfd29df07674bc4aa960cf661b5acd2
    {"a": 0}

    $ signac statepoint 9bfd29
    {"a": 0}


.. note::

    Using an abbreviated job id may result in multiple matches and is primarily designed for interactive use.


Find jobs
---------

Python:

.. code-block:: python

    # Iterate over all jobs in the data space
    for job in project:
        pass

    # Equivalent to
    for job in project.find_jobs():
        pass

    # Iterate over a data sub space with state point filter
    for job in project.find_jobs({'a': 0}):
        pass

    # Iterate over a data sub space with document filter
    for job in project.find_jobs(doc_filter={'a': 0}):
        pass

Command line:

.. code-block:: bash

    # Find all jobs
    $ signac find

    # Find a subset filtered by state point
    $ signac find '{"a": 0}'
    $ signac find a 0  # short form

    # Find a subset filtered by job document entries
    $ signac find --doc-filter '{"a": 0}'
    $ signac find --doc-filter a  0  # short form

.. note::

    The state point and document filter can be applied in combination.

Dataspace Operations
--------------------

A dataspace operation in the context of **signac projects** is defined as any process which creates, modifies or deletes project data as part of the project's dataspace.
Implemented in Python, such a operation should only require one argument, an instance of :py:class:`.Job`, in order to be well-defined:

.. code-block:: python

    def operate(job):
        pass

Execute in serial:

.. code-block:: python

    for job in project:
        operate(job)

    # or:
    list(map(operate, project))


Execute in parallel:

.. code-block:: python

    from multiprocessing import Pool
    with Pool() as pool:
        pool.map(operate, project)

    from multiprocessing.pool import ThreadPool
    with ThreadPool() as pool:
        pool.map(operate, project)

    from signac.contrib.mpipool import MPIPool
    with MPIPool() as pool:
        pool.map(operate, project)

Indexing
========

An index is collection of documents which describe an existing data space.

Generate a file index
---------------------

.. code-block:: python

    singac.index_files('/data', '.*\.txt')

Generate a signac project index
-------------------------------

Python:

.. code-block:: python

    project.index('.*\.txt')

Command line:

.. code-block:: bash

    $ signac project --index

Create an access module:
------------------------

Python:

.. code-block:: python

    project.create_access_module()

Command line:

.. code-block:: bash

    $ touch signac_access.py
    $ # or:
    $ signac project --access

Generate a master index
-----------------------

Python:

.. code-block:: python

    signac.index('/data')

Command line:

.. code-block:: bash

    $ signac index

Fetch Data
----------

Fetch files from an index document with :py:func:`.fetch`:

.. code-block:: python

    for doc in index:
        with signac.fetch(doc) as file:
            print(file.read())

Collections
===========

A :py:class:`.Collection` is a set of documents (mappings of key-value pairs).

Initialize a collection
-----------------------

.. code-block:: python

    # Directly in-memory:
    collection = signac.Collection(docs)

    # Associated with a file object:
    with Collection.open('index.txt') as collection:
        pass

Setup a command line interface
------------------------------

.. code-block:: python

    # find.py
    with signac.Collection.open('collection.txt') as collection:
        collection.main()

Iterate through a collection
----------------------------

Python:

.. code-block:: python

    for doc in collection:
        print(doc)

Command line:

.. code-block:: bash

    $ python find.py

Search for documents
--------------------

Python:

.. code-block:: python

    for doc in collection.find({'a': 42}):
        print(doc)

Command line:

.. code-block:: bash

    $ python find.py '{"a": 42}'

Database Integration
====================

The **signac** framework allows for the simple integration of databases, for example for the management of index collections.

Access a database
-----------------

.. code-block:: python

    db = signac.get_database('my_database')

Search a database collection
----------------------------

.. code-block:: python

    # a > 0
    docs = db.index.find({'a': {'$gt': 0}})

    # a = 2
    doc = db.index.find_one({'a': 2})
