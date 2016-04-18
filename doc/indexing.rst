.. _indexing:

========
Indexing
========

Concept
=======

To create a homogeneous data access layer, **signac** encourages the creation of a data index.
The data index contains all information about the project data structure and can be stored in a database or data frame which then allows the execution of query and aggregation operations on the data.

**signac** uses crawlers to `crawl` through a data source to generate an index.
A crawler is defined to generate a sequence of index documents (mappings), the data source is arbitrary.
Each index document requires at least one ``_id`` keyword.
For example, this would be a valid (albeit not useful) crawler:

.. code-block:: python

    my_source = ['a', 'b', 'c']

    class MyCrawler(object):
        def crawl(self):
            for i, x in enumerate(my_source):
                yield dict(_id=i, x=x)

In addition, we can link any kind of data to the documents produced in this way with the :py:meth:`~signac.contrib.BaseCrawler.fetch` method.
This method generates a sequence of arbitrary objects (usually file-like objects) as a function of the document.

.. code-block:: python

    class MyCrawler(object):
        # [..]
        def fetch(self, doc, mode='r'):
            yield open('/foo/bar/a_{}.txt'.format(doc['a']), mode)
            yield open('/foo/bar/a_{}.dat'.format(doc['a']), mode)

Indexing by filename
--------------------

A particular easy way to index files by filename is to use `regular expressions`_.
For this purpose signac provides the :py:class:`~signac.contrib.RegexFileCrawler`.

.. _`regular expressions`: https://en.wikipedia.org/wiki/Regular_expression

The `RegexFileCrawler` uses regular expressions to generate data from files.
This is a particular easy method to retrieve meta data associated with files.
Inherit from this class to configure a crawler for the given data structure.

Assuming we wanted to index text files, with a specific naming pattern, which
specifies a parameter `a` via the filename, e.g.:

.. code-block:: bash

    /data/my_project/a_0.txt
    /data/my_project/a_1.txt
    ...

To extract meta data for this filename structure, we create a regex pattern like this:

    ``a_(?P<a>\d+)\.txt``


This regular expression matches all filenames which begin with `a_`, followed by one or more digits, ending with `.txt`.
The definition of a named group, `a`, matching only the digits allows the :py:class:`~signac.contrib.crawler.RegexFileCrawler` to extract the meta data from this filename.
For example:

    ``a_0.txt -> {'a': 0}``
    ``a_1.txt -> {'a': 1}``
    ... and so on.

Each pattern is then associated with a specific format through the :py:meth:`~signac.contrib.RegexFileCrawler.define` class method.
We can use any class, as long as its constructor expects a `file-like object`_ as its first argument.
This would be a minimal implementation of such a format:

.. code-block:: python

    class TextFile(object):

        def __init__(self, file):
            self.data = file.read()

The final implementation of crawler then looks like this:

.. code-block:: python

    class MyCrawler(RegexFileCrawler):
        pass

    # This expressions yields mappings of the type: {'a': value_of_a}.
    MyCrawler.define('a_(?P<a>\d+)\.txt', TextFile)

In this case we could also use :class:`.contrib.formats.TextFile`
as data type which is a more complete implementation of the minimal example shown above.

.. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object

The index is then generated through the :py:meth:`~signac.contrib.BaseCrawler.crawl` method and can be stored in a database collection:

.. code-block:: python

   crawler = MyCrawler('/data/my_project')
   db.index.insert_many(crawler.crawl())

.. hint::

    Use the optimized export functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo` for more efficient export and avoidance of duplicates.

Indexing a signac project
-------------------------

To index a signac project we can either specialize a :py:class:`~signac.contrib.SignacProjectCrawler` or use the :py:meth:`~signac.contrib.project.Project.index` method.
The index will always contain all non-empty job documents.
To additionally index files we need to provide the filename pattern and format definition.

.. code-block:: python

    for doc in project.index(formats={'*\.txt': TextFile})
        print(doc)

Each index document contains the state point parameters stored under the ``statepoint`` keyword.

This is the same example with a :py:class:`~signac.contrib.SignacProjectCrawler`:

.. code-block:: python

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', Textfile)

    project = signac.get_project()
    crawler = MyCrawler(project.workspace())
    for doc in crawler.crawl():
        print(doc)

Notice that we used the regular expression to identify the text files that we want to index, but not to identify the state point.
However we can further extend the meta data using regular expressions to further diversify data within the state point data space.
An expression such as ``.*\(?P<class>init|final)\.txt`` will only match files named ``init.txt`` or ``final.txt``, and will add a field ``class`` to the index document, which will either have the value ``init`` or ``final``.

Master crawlers
===============

About
-----

A :py:class:`~signac.contrib.MasterCrawler` compiles a master index by combining all documents from other crawlers.
In this context those crawlers are called *slave crawlers*.
Any crawler (including other master crawlers) can be *slave crawlers*.

The *master crawler* adds information about its origin to each document.
This allows to fetch data from the *master index*, which is almost independent of the actual location of the data within the file system.

.. _signac-access:

The *signac_access.py* module
-----------------------------

The master crawler searches for modules called ``signac_access.py`` and tries to call a function called ``get_crawlers()`` defined in those modules.
This function is defined as follows:

.. py:function:: signac_access.get_crawlers(root)
    :noindex:

    Return crawlers to be executed by a master crawler.

    :param root: The directory where this module was found.
    :type root: str
    :returns: A mapping of crawler id and crawler instance.

By putting the crawler definitions from above into a file called *signac_access.py* and adding the ``get_crawlers()`` function, we make those crawlers available to a master crawler:

.. code-block:: python

     # signac_acess.py

     # [definitions as shown above]

     def get_crawlers(root):
        return {'main': MyCrawler(os.path.join(root, 'data'))}

The root argument is the absolute path to the location of the *signac_access.py* file, usually the project's root directory.
The *crawler id*, here ``main``, is a completely arbitrary string, however should not be changed after creating the index.

.. tip::

    Use the :py:meth:`~signac.contrib.project.Project.create_access_module` method to create the access module file for signac projects.

The master crawler is then executed for the indexed data space.

.. code-block:: python

    master_crawler = signac.contrib.MasterCrawler('/projects')
    signac.contrib.export_pymongo(master_crawler.crawl(depth=1), index)

.. warning::

    Especially for master crawlers it is recommended to reduce the crawl depth to avoid too extensive crawling operations over the *complete* file system.

Fetching data
-------------

As described above, a crawler generates a sequence of documents, where each document may be associated with an arbitrary sequence of objects.
The :py:class:`~signac.contrib.RegexFileCrawler` generates one document per matched file and associates that file with the respective document;
that is a *one-to-one* association.

We then use the :py:func:`signac.fetch` function to fetch data associated with a document:

.. code-block:: python

    # Get a document from the index:
    doc = index.find_one()

    # Fetch all files associated with this document:
    files = signac.fetch(doc)

When we *know* that a particular crawler, such as the :py:class:`~signac.contrib.RegexFileCrawler`, only yields one file per document, it is more convenient to use the :py:func:`~signac.fetch_one` function:

.. code-block:: python

    file = signac.fetch_one(doc)

Examples for *signac_access.py*
-------------------------------

This is a minimal example for a ``signac_access.py`` file using a :py:class:`~signac.contrib.RegexFileCrawler`:

.. code-block:: python

    # signac_access.py
    import os

    import signac
    from signac.contrib.formats import TextFile


    # Define a crawler class for each structure
    class MyCrawler(signac.contrib.RegexFileCrawler):
      pass

    # Add file definitions for each file type, that should be part of the index.
    MyCrawler.define('.*/a_(?P<a>\d+)\.txt', TextFile)

    # Expose the data structures to a master crawler
    def get_crawlers(root):
      # the crawler id is arbitrary, but should not be changed after index creation
      return {'main': MyCrawler(os.path.join(root, 'my_project'))}

This is a minimal example for a ``signac_access.py`` file using a :py:class:`~signac.contrib.SignacProjectCrawler`:

.. code-block:: python

    # signac_access.py
    import os

    import signac
    from signac.contrib.formats import TextFile

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', Textfile)

    def get_crawlers(root):
        return {'main': MyCrawler(os.path.join(root, 'path/to/workspace'))}

.. note::

    The root argument for a signac project crawler should be the project's **workspace**.

Advanced Indexing
=================

.. sidebar:: Warning: SYSADMIN ZONE

    The following topics are considered *advanced* and most likely only interesting to system administrators.

.. _data_mirroring:

Data mirroring
--------------

A **master crawler** will add a special field called `signac_link` to each crawled document.
This link allows to fetch all data exported by the **slave crawler** which was used to crawl the document in the first place.
This is why generating a *master index* and fetching data from it usually does not require any additional action.
However, in heterogeneous environments it is sometimes necessary to mirror the data provided by the *slave crawlers*.

For this purpose it is possible to pass *file system handlers* to the *master crawler*.
**signac** provides handlers for a local file system and the MongoDB `GridFS`_ database file system.
Please see :py:mod:`signac.contrib.filesystems` for details.

.. _`GridFS`: https://docs.mongodb.org/manual/core/gridfs/

To mirror to another file system, simply add the file system as argument to the *master crawler's* constructor:

.. code-block:: python

    from signac.contrib.filesystems import LocalFS

    MasterCrawler(
      root,
      mirrors = [LocalFS('/path/to/data/storage')])

Instead of passing the handlers directly, we can use a config dictionary.
Here are some examples using dictionaries to configure file systems:

.. code-block:: python

    MasterCrawler(root, mirrors=[{'localfs': '/path/to/data/storage'}])
    MasterCrawler(root, mirrors=[{'gridfs': 'my_database'}])

The key specifies the type of file system handler, the values are the arguments to the handler's constructor.
Please see :py:func:`~signac.contrib.filesystems.filesystems_from_config` for details.

Optimization
------------

When exporting to a database, such as MongoDB it is more efficent to use specialized export functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo`:

.. code-block:: python

    signac.contrib.export_pymongo(master_crawler.crawl(depth=1), db.master_index)

The functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo` are optimized for exporting to an index collection, ensuring that the collection does not contain any duplicates.
The behavior of these functions is roughly equivalent to

.. code-block:: python

    for doc in crawler.crawl(*args, **kwargs):
        index.replace_one({'_id': doc['_id']}, doc)

Tagging
-------

It may be desirable to only index select projects for a specific index for example to distinguish between public and private indexes.
For this purpose it is possible to provide  a set of tags to any crawler, as such:

.. code-block:: python

    class MyCrawler(SignacProjectCrawler):
        tags = {'public', 'foo'}


Master crawlers will ignore all crawlers with defined tags, that do not match *at least one* tag, e.g.:

.. code-block:: python

    # Any of the following master crawlers would ignore MyCrawler:
    master_crawler.tags = None
    master_crawler.tags = {}
    master_crawler.tags = {'private'}
    # or any other set of tags that does not contain either 'public' or 'foo' or both.

    # These master crawlers would execute MyCrawler:
    master_crawler.tags = {'public'}
    master_crawler.tags = {'foo'}
    master_crawler.tags = {'foo', 'public'}
    master_crawler.tags = {'private', 'foo'}

Creating a public index
-----------------------

Here we demonstrate how to generate a master index, accessible to the public using MongoDB.
As public users will most likely have no access to the local file system, it is necessary to mirror the data.
Most conveniently the data is stored directly in the database using GridFS.

.. code-block:: python

    db = signac.get_database('public_db')

    master_crawler = MasterCrawler(
      # The project root path
      root='/path/to/projects/',

      # The following argument suppresses the creation
      # of the default link, which is of no use
      # without access to the local file system.
      link_local=False,

      # We define two extra mirrors:
      mirrors = [
        # The GridFS database file system is stored in the
        # same database, that we use to publish the index.
        # This means that anyone who can access the index,
        # will be able to access the associated files.
        {'gridfs': 'public_db'},

        # The second mirror is on the local file system.
        # It can be downloaded and made available locally,
        # for example to reduce required network transfers.
        {'localfs': '/path/to/mirror'}
        ]
      )


    # By defining special tags for projects, which are cleared
    # for publication, we prevent the accidental export of private
    # data to the database.
    master_crawler.tags = {'public'}

    signac.contrib.export_pymongo(master_crawler.crawl(depth=1), index_collection)

To access the data, we simply execute:

.. code-block:: python

    for doc in index.find():
        files = signac.fetch(doc)

If we have a local mirror of the data, we need to tell ``fetch()`` to use it.
This is most conveniently achieved by defining two wrapper functions:

.. code-block:: python

    sources = [
      {'localfs': '/path/to/mirror'},
      {'gridfs': 'gridfsdb'}]

    def fetch(*args, **kwargs):
        yield from signac.fetch(sources=sources, *args, **kwargs)

    def fetch_one(*args, **kwargs):
        return signac.fetch(sources=sources, *args, **kwargs)

.. note::

    File systems are used to fetch data in the order provided.
    For the example given above, the local source will be queried *first*.
    Only if files cannot be fetched using the local source, other sources
    will be queried.
