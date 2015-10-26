==========
Quickstart
==========

Signac projects
===============

Specifying a project name as identifier within a configuration file initiates a signac project.

.. code:: bash

    $ mkdir my_project
    $ cd my_project
    $ echo project=MyProject >> signac.rc

The directory that contains this configuration file is the project's root directory.
You can specify a workspace directory that signac uses to store job data in.

.. code:: bash

    $ echo workspace_dir=/path/to/my/workspace/for/my_project >> signac.rc

You can access your signac :class:`~signac.contrib.project.Project` from within your project's root directory or any subdirectory with the :py:func:`~signac.contrib.get_project` function.

.. code:: python

    $ python
    >>> import signac
    >>> project = signac.contrib.get_project()
    >>> print(project)
    MyProject

You can use the project to store data associated with a unique set of parameters.
Parameters are defined by a mapping of key-value pairs stored for example in a :py:class:`dict` object.
Each statepoint is a associated with a unique hash value, called *job id*.
Get an instance of :py:class:`~signac.contrib.job.Job`, which is a handle on your job's data space with the :py:meth:`~signac.contrib.project.Project.open_job` method.

.. code:: python

    # define a statepoint
    >>> statepoint = {'a': 0}
    # get the associated job
    >>> job = project.open_job(statepoint)
    >>> job.get_id()
    '9bfd29df07674bc4aa960cf661b5acd2'

You can use the job id to organize your data.
If you configured a workspace directory for your project you can obtain a unique path for each job.

.. code:: python

    >>> job.workspace()
    '/path/to/my/workspace/for/my_project/9bfd29df07674bc4aa960cf661b5acd2'

Database
========

Accessing the database
----------------------

After :doc:`configuring <configuration>` one ore more database hosts you can access a database with the :py:func:`signac.db.get_database` function.

.. automodule:: signac.db
    :members:
    :undoc-members:
    :show-inheritance:


Database queries and aggregation
--------------------------------

To execute queries on a :py:class:`~pymongo.collection.Collection` instance use the :py:meth:`~pymongo.collection.Collection.find` or :py:meth:`~pymongo.collection.Collection.find_one` methods.

Aggregation pipelines are executed with the :py:meth:`~pymongo.collection.Collection.aggregate` method.

.. seealso:: https://api.mongodb.org/python/current/api/pymongo/collection.html

Indexing data
-------------

Crawlers `crawl` through a data source and generate an index which can then be operated on with database query and aggregation operations.
Signac expects a crawler to produce a series of JSON documents, the data source is arbitrary.

Every crawler provided by the signac package inherits from :py:class:`~signac.contrib.crawler.BaseCrawler`, which crawls through files stored in a filesystem.
A particular easy way to index files is to use `regular expressions`_.
For this purpose signac provides the :py:class:`~signac.contrib.crawler.RegexFileCrawler`.

.. _`regular expressions`: https://en.wikipedia.org/wiki/Regular_expression

Processing data
---------------

Processing data always consists of three steps:

  1. Get the input.
  2. Process the input to produce output.
  3. Store the output.

First, we define our processing function:

.. code:: python

   def process(doc):
      doc['calc_value'] =  # ...
      return doc

In this case we effectively copied and extended the input document to produce the output document which has the benefit of preserving all metadata, but is not strictly necessary.
Then, we fetch our input documents from a collection that contains the index, in this case called `index`:

.. code:: python

   db = signac.db.get_database('MyProject')
   docs = db.index.find({'a': {'$lt': 100}})

We can use the :py:func:`map` function to generate the results:

.. code:: python

   results = map(process, docs)

and store them in a result collection called `results`:

.. code:: python

   db.results.insert_many(results)

By using a different map function, we can **trivially** parallelize this process, for example with a process pool:

.. code:: python

   import multiprocessing

   with multiprocessing.Pool(8) as pool:
     results = pool.imap(process, docs)

or an MPI pool, which is bundled with signac:

.. code:: python

   with signac.contrib.MPIPool() as pool:
      results = pool.map(process, docs, ntask=docs.count())

We can then operate with the results collection and for example reduce the data with aggregation operations.
This is an example of how we could calculate the average of our calculated value grouped by a second parameter b:

.. code::

    reduced_result = db.results.aggregate(
      [
        {'$group: {
            '_id': 'b',
            'avgValue': {'$avg': 'calc_value'}
            }
        }
      ]
    )

.. seealso::

  The combined process of mapping and reducing is called MapReduce_.
  For more information on the aggregation syntax, please refer to the `MongoDB reference on aggregation`_.

.. _MapReduce: https://en.wikipedia.org/wiki/MapReduce
.. _`MongoDB reference on aggregation`: https://docs.mongodb.org/manual/reference/aggregation/
