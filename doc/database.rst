.. _database_integration:

Database Integration
====================

Database access
---------------

After :doc:`configuring <configuration>` one or more database hosts you can access a database with the :py:func:`signac.get_database` function.

.. autofunction:: signac.get_database
    :noindex:

Queries and aggregation
-----------------------

To execute queries on a :py:class:`~pymongo.collection.Collection` instance use the :py:meth:`~pymongo.collection.Collection.find` or :py:meth:`~pymongo.collection.Collection.find_one` methods.

Aggregation pipelines are executed with the :py:meth:`~pymongo.collection.Collection.aggregate` method.

.. seealso:: https://api.mongodb.org/python/current/api/pymongo/collection.html

Basic mapping
-------------

Processing data always consists of three steps:

  1. Fetch the input.
  2. Process the input to produce output.
  3. Store the output.

First, we define our processing function:

.. code-block:: python

   def process(doc):
      doc['calc_value'] =  # ...
      return doc

In this case we effectively copy and extend the input document to produce the output document which has the benefit of preserving all meta data, but is not strictly necessary.

We fetch our input documents from a collection that contains the index, in this case called `index`:

.. code-block:: python

   db = signac.db.get_database('MyProject')
   docs = db.index.find({'a': {'$lt': 100}})

We can use the :py:func:`map` function to generate the results:

.. code-block:: python

   results = map(process, docs)

and store them in a result collection called `results`:

.. code-block:: python

   db.results.insert_many(results)

By using a different map function, we can **trivially** parallelize this process, for example with a process pool:

.. code-block:: python

   import multiprocessing

   with multiprocessing.Pool(8) as pool:
     results = pool.imap(process, docs)

or an MPI pool, which is bundled with signac:

.. code-block:: python

   with signac.contrib.MPIPool() as pool:
      results = pool.map(process, docs, ntask=docs.count())
