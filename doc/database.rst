========
Database
========

Accessing the database
======================

After :doc:`configuring <configuration>` one ore more database hosts you can access a database with the :py:func:`signac.db.get_database` function.

.. automodule:: signac.db
    :members:
    :undoc-members:
    :show-inheritance:


Database queries and aggregation
================================

To execute queries on a :py:class:`~pymongo.collection.Collection` instance use the :py:meth:`~pymongo.collection.Collection.find` or :py:meth:`~pymongo.collection.Collection.find_one` methods.

Aggregation pipelines are executed with the :py:meth:`~pymongo.collection.Collection.aggreagate` method.

.. seealso:: https://api.mongodb.org/python/current/api/pymongo/collection.html

Indexing data
=============

Concept
-------

Use crawlers to create an index on your data, which you can then store in a database.
Crawlers `crawl` through a data source and generate an index which can then be operated on with database query and aggregation operations.

Signac expects a crawler to produc an iterable of JSON documents, the data source is arbitrary.

File indexing
-------------

Every crawler provided by the signac package inherits from :py:class:`~signac.contrib.crawler.BaseCrawler`, which crawls through the files stored on a filesystem.

A particular easy way to index files is to use `regular expressions`_.

.. _`regular expressions`: https://en.wikipedia.org/wiki/Regular_expression

For this purpose signac provides the :py:class:`~signac.contrib.crawler.RegexFileCrawler`.
