=================
Advanced Indexing
=================

The Crawler Concept
===================

**signac** uses crawlers to *crawl* through a data source to generate an index.
A crawler is defined to generate a sequence of index documents (mappings), the data source is arbitrary.
Each index document requires at least one ``_id`` keyword.
For example, this would be a valid crawler:

.. code-block:: python

    my_source = ['a', 'b', 'c']

    class MyCrawler(object):

        def crawl(self):
            for i, x in enumerate(my_source):
                yield dict(_id=i, x=x)

This crawler would generate the following documents:

.. code-block:: python

    >>> for doc in MyCrawler().crawl():
    ...     print(doc)
    ...
    {'x': 'a', '_id': 0}
    {'x': 'b', '_id': 1}
    {'x': 'c', '_id': 2}

The :py:func:`.index`, :py:func:`.index_files`, or :py:meth:`.Project.index` functions, internally define a *Crawler* class that is then executed to generate the index.
These crawlers are subclassed from either :py:class:`.RegexFileCrawler`, :py:class:`.SignacProjectCrawler`, or :py:class:`.MasterCrawler`.

Customizing Crawlers
====================

Defining our own ``Crawler`` class provides us with full control over the index creation.
For example, imagine we wanted to add an additional field to the index, that contains the length of each indexed file, we could define the following crawler class:

.. code-block:: python

    class MyCrawler(signac.RegexFileCrawler):

        def process(self, doc, dirpath, fn):
            with open(os.path.join(dirpath, fn)) as file:
                doc['size'] = len(file.read())
            return super(MyCrawler, self).process(doc, dirpath, fn)

    MyCrawler.define('.*\.txt')

In this example, we define a subclass of :py:class:`.RegexFileCrawler` called ``MyCrawler`` and redefine the :py:meth:`~.RegexFileCrawler.process` method to add a ``size`` field to each generated document.
We could put this definition into a ``signac_access.py`` module and make it part of a master index like this:

.. code-block:: python

    import signac

    class MyCrawler(signac.contrib.RegexFileCrawler):
        # ...

    def get_indexes(root):
        yield MyCrawler(root).crawl()

.. _data_mirroring:

Mirroring of Data
=================

Using the :py:func:`signac.fetch` function it is possible retrieve files that are associated with index documents.
Those files will preferably be opened directly via a local system path.
However in some cases it may be desirable to mirror files at a different location, e.g., in a database or a different path to increase the accessibility of files.

Use the mirrors argument in the :py:func:`signac.export` function to automatically mirror all files associated with exported index documents.
**signac** provides handlers for a local file system and the MongoDB `GridFS`_ database file system.

.. code-block:: python

    from signac import fs, export, get_database

    db = get_database('mirror')

    localfs = fs.LocalFS('/path/to/mirror')
    gridfs = fs.GridFS(db)

    export(crawler.crawl(), db.index, mirrors=[localfs, gridfs])

.. _`GridFS`: https://docs.mongodb.org/manual/core/gridfs/


To access the data, provide the mirrors argument to the :py:func:`signac.fetch` function:

.. code-block:: python

    for doc in index:
        with signac.fetch(doc, mirrors=[localfs, gridfs]) as file:
            do_something_with_file(file)

.. note::

    File systems are used to fetch data in the order provided, starting
    with the native data path.


Using Tags to Control Access
============================

It may be desirable to only index select projects for a specific *master index*, e.g., to distinguish between public and private indexes.
For this purpose, it is possible to specify **tags** that are **required** by a *crawler* or *index*.
This means that an index **requiring** tags will be ignored during a master index compilation, unless at least one of the tags is also **provided**.

For example, you can define **required** tags for indexes returned from the ``get_indexes()`` function, by attaching them to the function like this:

.. code-block:: python

    def get_indexes(root):
        yield signac.get_project(root).index()

    get_indexes.tags = {'public', 'foo'}

Similarly, you can require tags for specific crawlers:

.. code-block:: python

    class MyCrawler(SignacProjectCrawler):
        tags = {'public', 'foo'}

Unless you **provide** *at least one* of these tags (``public`` or ``foo``), the examples above would be ignored during the master index compilation.
This means only the second one of the following two lines would **not ignore** the examples above:

.. code-block:: python

    index = signac.index()                  # examples above are ignored
    index = signac.index(tags={'public'})   # includes examples above

Similarly on the command line:

.. code-block:: bash

    $ signac index                # examples above are ignored
    $ signac index --tags public  # includes examples above

In summary, there must be an overlap between the **requested** and the **provided** tags.

How to publish an index
=======================

Here we demonstrate how to compile a master index with data mirroring, which is designed to be publicly accessible.
The index will be stored in a document collection called ``index`` as part of a database called ``public_db``.
All data files will be mirrored within the same database.
That means everybody with access to the ``public_db`` database will have access to the index as well as to the associated files.

.. code-block:: python

    import signac

    db = signac.get_database('public_db')

    # We define two mirrors
    file_mirrors = [
      # The GridFS database file system is stored in the
      # same database, that we use to publish the index.
      # This means that anyone with access to the index,
      # will be able to access the associated files as well.
      signac.fs.GridFS(db),

      # The second mirror is on the local file system.
      # It can be downloaded and made available locally,
      # for example to reduce the amount of required
      # network traffic.
      signac.fs.LocalFS('/path/to/mirror')
      ]

    # Only crawlers which have been explicitly cleared for
    # publication with the `public` tag will be compiled and exported.
    index = signac.index('/path/to/projects', tags={'public'})

    # The export() function pushes the index documents to the database
    # collection and copies all associated files to the file mirrors.
    signac.export(index, db.index, file_mirrors, update=True)
