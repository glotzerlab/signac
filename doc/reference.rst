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

    with project.open_job({'a': 0}) as job:
        # do your job...

     with project.open_job(id='9bfd29df07674bc4aa960cf661b5acd2') as job:
        # do your job...

.. code-block:: bash

    $ signac job '{"a": 0}'
    9bfd29df07674bc4aa960cf661b5acd2
    $ signac job --workspace '{"a": 0}'
    /path/to/workspace/9bfd29df07674bc4aa960cf661b5acd2
    $ signac statepoint 9bfd29df07674bc4aa960cf661b5acd2
    {"a": 0}

Find jobs
---------

.. code-block:: python

    # Iterate over all jobs in the data space:
    for job in project.find_jobs():
        # ...

    # Iterate over a data sub set:
    for job in project.find_jobs({'a': 0}):
        # ...

.. code-block:: bash

    # Find all jobs:
    $ signac find

    # Find a filtered subset:
    $ signac find '{"a": 0}'

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
