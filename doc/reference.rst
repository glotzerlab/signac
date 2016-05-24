.. _quickreference:

Quick Reference
===============

Start a new project
-------------------

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

.. code-block:: shell

    $ signac job '{"a": 0}'
    9bfd29df07674bc4aa960cf661b5acd2
    $ signac job --workspace '{"a": 0}'
    /path/to/workspace/9bfd29df07674bc4aa960cf661b5acd2


Access a database
-----------------

.. code-block:: python

    db = signac.get_database('my_database')

Search a database collection
----------------------------

Example for a collection named *index*:

.. code-block:: python

    # a > 0
    docs = db.index.find({'a': {'$gt': 0}})

    # a = 2
    doc = db.index.find_one({'a': 2})

Index project data
------------------

1. Use the :py:meth:`~signac.contrib.project.Project.index` method:

    .. code-block:: python

        for doc in project.index():
            print(doc)

2. Export the index to a database collection:

    .. code-block:: python

        db = signac.get_database('mydb')
        signac.contrib.export(project.index(), db.index)  # or export_pymongo()

3. Create a ``signac_access.py`` module with the :py:meth:`~signac.contrib.project.Project.create_access_module` method (or :ref:`manually <signac-access>`)  to expose the index to a :py:class:`~signac.contrib.crawler.MasterCrawler`.

Access data using an index
--------------------------

Access files using an index with :py:func:`signac.fetch` and :py:func:`signac.fetch_one`:

.. code-block:: python

    docs = db.index.find({'a': 0, 'format': {'$regex': 'TextFile'}})
    for doc in docs:
        with signac.fetch_one(doc) as file:
            print(file.read())
