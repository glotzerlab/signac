.. _quickreference:

Quick Reference
===============

Start a new project
-------------------

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject


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

1. Create a ``signac_access.py`` module:

  .. code-block:: python

    # signac_access.py
    import os

    import signac
    from signac.contrib.formats import TextFile

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', TextFile)

    def get_crawlers(root):
        return {'main': MyCrawler(os.path.join(root, 'path/to/workspace'))}

2. Export the index using a :py:class:`~signac.contrib.MasterCrawler`:

  .. code-block:: python

      master_crawler = signac.contrib.MasterCrawler('/path/to/projects/')
      signac.contrib.export_pymongo(master_crawler, db.index, depth=1)

Access data using an index
--------------------------

Access files using an index with :py:func:`signac.fetch` and :py:func:`signac.fetch_one`:

.. code-block:: python

    docs = db.index.find({'a': 0, 'format': {'$regex': 'TextFile'}})
    for doc in docs:
        with signac.fetch_one(doc) as file:
            print(file.read())
