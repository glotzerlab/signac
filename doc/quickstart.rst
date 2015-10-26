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
=============

Use crawlers to create an index on your data, which can then be stored in a database.
Crawlers `crawl` through a data source and generate an index which can then be operated on with database query and aggregation operations.
Signac expects a crawler to produc an iterable of JSON documents, the data source is arbitrary.

Every crawler provided by the signac package inherits from :py:class:`~signac.contrib.crawler.BaseCrawler`, which crawls through the files stored on a filesystem.
A particular easy way to index files is to use `regular expressions`_.
For this purpose signac provides the :py:class:`~signac.contrib.crawler.RegexFileCrawler`.

.. _`regular expressions`: https://en.wikipedia.org/wiki/Regular_expression


Processing data
===============

Processing data consists of three steps:

  1. Get the input.
  2. Process the input.
  3. Store the output.


First, we define our processing function

.. code:: python

   def process(doc):
      doc['calc_value'] =  # ...
      return doc

Then, we fetch our input documents

.. code:: python

   db = signac.db.get_database('MyProject')
   docs = db.find({'a': {'$lt': 100}})

Finally, we process the input

.. code:: python

   results = map(process, docs)

and store the results

.. code:: python

   db.results.insert_many(results)

We can **instantly** parallelize this process, by using a process pool:

.. code:: python

   import multiprocessing

   with multiprocessing.Pool(8) as pool:
     results = pool.imap(process, docs)

or an MPI pool, provided by signac:

.. code:: python

   with signac.contrib.MPIPool() as pool:
      results = pool.map(process, docs, ntask=docs.count())

Map-Reduce
==========

One way to process data in the database is the map-reduce schema:

  1. Create a query, to define the data set to operate on,
  2. process (map) the data into a new set of data,
  3. reduce the data.

In our example we want to process all data, where the parameter `a` is less than 100.
We will calculate a value from our document and store the result in a second collection called `results`.
As the final step, we will reduce the data by calculating the average of all calculated values grouped by a second parameter `b`.

.. code::

    import signac

    def process(doc):
        doc['calculated_value'] = # ...
        return doc

    db = signac.db.get_database('MyProject')
    query = {'a': {'$lt': 100}}
    db.results.insert_many(map(process, db.index.find(query)))
    the_average = db.results.aggregate([
      {'$group': {
        '_id': 'b',
        'avgValue': {'$avg': 'calculated_value'}
    }}])
