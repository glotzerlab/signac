.. _quickreference:

===============
Quick Reference
===============

Projects
========

Start a new project
-------------------

.. code-block:: python

   project = signac.init_project('MyProject')

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject
    Initialized project 'MyProject'.

Open a job
----------

.. code-block:: python

    project = signac.get_project()

    # Open with state point
    with project.open_job({'a': 0}) as job:
        pass

     # Open with job id
     with project.open_job(id='9bfd29df07674bc4aa960cf661b5acd2') as job:
        pass

     # Open with abbreviated id (must be long enough to be unique)
     with project.open_job(id='9bfd29') as job:
        pass


.. code-block:: bash

    $ signac job '{"a": 0}'
    9bfd29df07674bc4aa960cf661b5acd2

    $ signac job --workspace '{"a": 0}'
    /path/to/workspace/9bfd29df07674bc4aa960cf661b5acd2

    $ signac statepoint 9bfd29df07674bc4aa960cf661b5acd2
    {"a": 0}

    $ signac statepoint 9bfd29
    {"a": 0}


Find jobs
---------

.. code-block:: python

    # Iterate over all jobs in the data space
    for job in project:
        pass

    # Equivalent to
    for job in project.find_jobs():
        pass

    # Iterate over a data sub space with filter
    for job in project.find_jobs({'a': 0}):
        pass

.. code-block:: bash

    # Find all jobs
    $ signac find

    # Find a subset filtered by state point
    $ signac find '{"a": 0}'

    # Find a subset filtered by job document entries
    $ signac find --doc-filter '{"a": 0}'

Index project data
------------------

1. Use the :py:meth:`~signac.contrib.project.Project.index` method:

    .. code-block:: python

        for doc in project.index():
            print(doc)

2. Use the ``signac index`` function:

    .. code-block:: bash

        $ signac index

3. Define a custom crawler for example for a ``signac_access.py`` module:

    .. code-block:: python

        project.create_access_module()

Dataspace Operations
====================

Definition
----------

.. code-block:: python

    def func(job):
        pass

Execute in serial
-----------------

.. code-block:: python

    for job in project:
        func(job)

    # or:
    list(map(func, project))


Execute in parallel
-------------------

.. code-block:: python

    from multiprocessing import Pool
    with Pool() as pool:
        pool.map(func, project)

    from multiprocessing.pool import ThreadPool
    with ThreadPool() as pool:
        pool.map(func, project)

    from signac.contrib.mpipool import MPIPool
    with MPIPool() as pool:
        pool.map(func, project)


Database Integration
====================

Access a database
-----------------

.. code-block:: python

    db = signac.get_database('my_database')

Export an index to a database collection
----------------------------------------

.. code-block:: python

    db = signac.get_database('mydb')
    signac.export(project.index(), db.index)

Search a database collection
----------------------------

Example for a collection named *index*:

.. code-block:: python

    # a > 0
    docs = db.index.find({'a': {'$gt': 0}})

    # a = 2
    doc = db.index.find_one({'a': 2})

Access data using an index
--------------------------

Access files using an index with :py:func:`signac.fetch`:

.. code-block:: python

    docs = db.index.find({'a': 0, 'format': 'TextFile'})
    for doc in docs:
        with signac.fetch(doc) as file:
            print(file.read())
