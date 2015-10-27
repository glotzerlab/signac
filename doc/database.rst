========
Database
========

Basics
======

Database access
---------------

After :doc:`configuring <configuration>` one ore more database hosts you can access a database with the :py:func:`signac.db.get_database` function.

.. automodule:: signac.db
    :members:
    :undoc-members:
    :show-inheritance:


Queries and aggregation
-----------------------

To execute queries on a :py:class:`~pymongo.collection.Collection` instance use the :py:meth:`~pymongo.collection.Collection.find` or :py:meth:`~pymongo.collection.Collection.find_one` methods.

Aggregation pipelines are executed with the :py:meth:`~pymongo.collection.Collection.aggregate` method.

.. seealso:: https://api.mongodb.org/python/current/api/pymongo/collection.html

Data indexing
=============

Concept
-------

Crawlers `crawl` through a data source and generate an index which can then be operated on with database query and aggregation operations.
Signac expects a crawler to produce a series of JSON documents, the data source is arbitrary.

Every crawler provided by the signac package inherits from :py:class:`~signac.contrib.BaseCrawler`, which crawls through files stored in a file system.

Indexing by filename
--------------------

A particular easy way to index files by filename is to use `regular expressions`_.
For this purpose signac provides the :py:class:`~signac.contrib.RegexFileCrawler`.

.. _`regular expressions`: https://en.wikipedia.org/wiki/Regular_expression

The `RegexFileCrawler` uses regular expressions to generate data from files.
This is a particular easy method to retrieve meta data associated with files.
Inherit from this class to configure a crawler for your data structure.

Assuming we wanted to index text files, with a specific naming pattern, which
specifies a parameter `a` via the filename, e.g.:

.. code-block:: bash

    /data/my_project/a_0.txt
    /data/my_project/a_1.txt
    ...

To extract meta data for this filename structure, we create a regex pattern like this:

    ``a_(?P<a>\d+)\.txt``


This regular expression matches all filenames which begin with `a_`, followed by one more digits, ending with `.txt`.
The definition of a named group, `a`, matching only the digits allows the :py:class:`~signac.contrib.RegexFileCrawler` to extract the meta data from our filename.
For example:

    ``a_0.txt -> {'a': 0}``
    ``a_1.txt -> {'a': 1}``
    ... and so on.

Each pattern is then associated with a specific format through the :py:func:`~signac.contrib.RegexFileCrawler.define` class method.
We can use any class, as long as its constructor expects a `file-like object`_ as its first argument.

.. code-block:: python

    class TextFile(object):

        def __init__(self, file):
            # file is a file-like object
            return file.read()

The final implementation of crawler then looks like this:

.. code-block:: python

    class TextFile(object):
        def __init__(self, file):
            # file is a file-like object
            return file.read()

    # This expressions yields mappings of the type: {'a': value_of_a}.
    RE_TXT = re.compile('a_(?P<a>\d+)\.txt')

    MyCrawler(RegexFileCrawler): pass
    MyCrawler.define(RE_TXT, TextFile)

In this case we could also use :class:`.contrib.formats.TextFile`
as data type which is an implementation of the example shown above.

.. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object

The index is then generated through the :py:meth:`~signac.contrib.RegexFileCrawler.crawl` method and can be stored in a database collection:

.. code-block:: python

   crawler = MyCrawler('/data/my_project')
   db.index.insert_many(crawler.crawl())

.. seealso::

    Because this is such a common pattern, this particular function has been optimized, please see :py:func:`~signac.contrib.export_pymongo`.

Indexing a signac project
-------------------------

Indexing signac projects is simplified by using a :py:class:`~signac.contrib.SignacProjectCrawler`.
In this case meta data is automatically retrieved from the state point as well as from the :py:meth:`signac.contrib.job.Job.document`.

Master crawlers
---------------

It his highly recommended to not execute crawlers directly, but rather use a so called :py:class:`~signac.contrib.MasterCrawler`, which tries to find other crawlers and automatically executes them.
In this way we don't need to care about the actual location of our data within our file system as long as the local hierarchy is preserved.
The master crawler searches for modules called `signac_access.py` and tries to call a function called `get_crawlers()`.
This function is defined as follows:

.. py:function:: signac_access.get_crawlers(root)
    :noindex:

    Return crawlers to be executed by a master crawler.

    :param root: The directory where this module was found.
    :type root: str
    :returns: A mapping of crawler id and crawler instance.

This is minimal example for a `signac_access.py` file:

.. code-block:: python

    # ~/signac_access.py
    import os
    import re

    import signac
    from signac.contrib.formats import TextFile


    # Define a crawler class for each structure
    MyCrawler(RegexFileCrawler): pass

    # Add file definitions for each file type, that should be part of the index.
    MyCrawler.define(re.compile('a_(?P<a>\d+\.txt'), TextFile)

    # Expose the data structures to a master crawler
    def get_crawlers(root):
      return {
        # the crawler name is arbitrary, but is required for identification
        'main': MyCrawler(os.path.join(root, 'my_project'))
        }

The master crawler is then executed for the indexed data space.

.. code-block:: python

    >>> master_crawler = signac.contrib.MasterCrawler('/data')
    >>> signac.contrib.export_pymongo(master_crawler, db.master_index, depth=1)

.. warning::

    Especially for master crawlers it is recommended to reduce the crawl depth to avoid too extensive crawling operations over the *complete* filesystem.

.. seealso::

    This usage pattern has been optimized, please see :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo`.

Processing data
===============

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

.. seealso::

   Data that were indexed by a :py:class:`~signac.contrib.MasterCrawler` can be fetched with the :py:func:`signac.contrib.fetch` function.

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

Reduction
---------

We can then operate with the results collection and for example reduce the data with aggregation operations.
This is an example of how we could calculate the average of our calculated value grouped by a second parameter b:

.. code-block:: python

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
