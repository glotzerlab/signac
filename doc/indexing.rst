.. _indexing:

========
Indexing
========

Create a project index
======================

**Under construction**

Create a file index
===================

**Under construction**

Fetch data
==========

**Under construction**

Deep Indexing
==============

**Under construction**

Master Indexes
==============

**Under construction**

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

















========
Indexing
========

Concept
=======

To create a homogeneous data access layer, **signac** encourages the creation of a data index.
The data index contains all information about the project's data structure and can be stored in a database or data frame which then allows the execution of query and aggregation operations on the data.

While **signac**'s project interface is specifically useful during the data curation and generation phase, working with indexes may be useful in later stages of an investigation.
Especially when data is curated from multiple different projects and sources or if data spaces do not use the signac project schema.

For example, we may want to calculate the average of some values that we read from files associated with a specific data sub space:

.. code-block:: python

    def extract_value(doc):
        with signac.fetch(doc) as file:
            return float(file.read())

    docs = index.find({'statepoint.a': 42})
    average = sum(map(extract_value, docs)) / len(docs)

The next few sections will outline in detail how such a workflow can be realized.

Generating a File Index
=======================

An index is a collection of index documents, where each index document is an arbitrary collection of metadata describing the data space.
In the specific case of a file index, each index document is associated with one file on disk and contains the following fields:

  * ``_id``: a unique value which serves as a primary key
  * ``root``: The root path of the file
  * ``filename``: The filename of the file
  * ``md5``: A MD5-hash value of the file content
  * ``file_id``: A number identifying the file content [#f2]_
  * ``format``: A format definition (optional)

.. [#f2] Identical with the ``md5`` value in the current implementation.

To create a file index, execute the :py:func:`~.index_files` function:

.. code-block:: python

    for doc in signac.index_files():
        print(doc)

With no arguments, the :py:func:`.index_files` function will index **all** files in the current working directory.
We can limit the indexing to specific files by specifying the root path and by providing a `regular expression pattern <https://en.wikipedia.org/wiki/Regular_expression>`_ that all filenames must match.
For example, to index all files in the ``/data`` directory that end in ``.txt``, execute:

.. code-block:: python

    for doc in signac.index_files('/data', '.*\.txt'):
        print(doc)

We can extract metadata directly from the filename by using regular expressions with *named groups*.
For example, if we have a filename pattern: ``a_0.txt``, ``a_1.txt`` and so on, where the number following ``a_`` is to be extracted as the ``a`` field, we can use the following expression:

.. code-block:: python

    for doc in signac.index_files('/data', '.*a_(?P<a>\d+)'):
        print(doc['a'])

To further simplify the selection of different files from the index, we may provide multiple patterns with an optional *format definition*.
Let's imagine we would like to classify the text files with the ``a`` field from the previous example and in addition index PDF-files that adhere to the following pattern: ``init.pdf`` or ``final.pdf``.

This is how we could generate the index:

.. code-block:: python

    formats = {
        '.*a_(?P<a>\d+)\.txt': 'TextFile',
        '.*(?P<class>init|final)\.pdf': 'PDFFile'}

    for doc in signac.index_files(formats=formats):
        print(doc)

.. tip::

    To generate regular expressions for the filename patterns in your data space, copy & paste a few representative filenames into the excellent `regex101`_ online app.
    That will allow you to work out your expressions while getting direct graphical feedback.

.. _`regex101`: https://regex101.com

Indexing a signac Project
=========================

A signac project index is like a regular file index, but contains the following additional fields:

  * ``signac_id``: The state point id the document is associated with.
  * ``statepoint``: The state point mapping associated with the file.

This means that we do not have to define regular expressions to extract the state point schema, but take advantage of the signac project schema for state points.
To generate a signac project index, execute the :py:meth:`.Project.index` method:

.. code-block:: python

    for doc in project.index():
        print(doc)

Each signac project index will have *at least one* entry for each initialized job.
This special index document is associated with the job's :ref:`document <project-job-document>` and contains not only the ``signac_id`` and the ``statepoint``, but also the data stored in the job document:

.. code-block:: python

    for job in project:
        job.document['foo'] = 'bar'

    for doc in project.index():
        assert doc['foo'] == 'bar'

Just like for regular file indexes generated with :py:func:`.index_files`, we can still define regular expressions to limit the indexing to specific files and to extract additional metadata.

Generating a Master Index
=========================

A master index is a compilation of multiple indexes, which simplifies the operation on a larger data space.
To make a signac project part of a master index, we simply create a file called ``signac_access.py`` in its root directory.
The existance of this file tells **signac** that the projects in those directories should be indexed as part of a master index.

Imagining that we have two projects in two different directories ``~/project_a`` and ``~/project_b`` within our home directory.
We then create the ``signac_access.py`` file in each respective project directory like this:

.. code-block:: bash

    $ touch ~/project_a/signac_access.py
    $ touch ~/project_b/signac_access.py

Executing the :py:func:`~.index` function for the home directory

.. code-block:: python

    for doc in signac.index('~'):
        print(doc)

will now yield a joint index for both projects in ``~/project_a`` and ``~/project_b``.

For more information on how to have more control over the index creation, see the :ref:`access-module` section.

.. tip::

  You can generate a signac master index directly on the command line with ``$ signac index``, which can thus be directly piped into a file:

  .. code-block:: bash

      $ signac index > index.txt

Managing Index Collections
==========================

Once we have generated an index, we can use it to search our data space.
For example, if we are looking for all files that correspond to a state point variable ``a=42``, we could implement the following for-loop:

.. code-block:: python

    index = project.index()

    docs = []
    for doc in index:
        if doc['statepoint']['a'] == 42:
          docs.append(doc)

This is the same logic implemented more concisely as a list comprehension:

.. code-block:: python

    docs = [doc for doc in index if doc['statepoint']['a'] == 42]

This is a very viable approach as long as the index is not too large and the search queries are relatively simple.
An alternative way to manage an index is to use a :py:class:`.Collection`.
For example, to execute the same search operation from above, we could use the :py:meth:`~.Collection.find` method:

.. code-block:: python

    index = Collection(signac.index())

    docs = index.find({'statepoint.a': 42})

.. sidebar:: Tip

    You can search a collection on the command line by calling it's :py:meth:`~.Collection.main` method.

Searching a collection is usually **much more efficient** compared to the *pure-python* approach especially when searching multiple times within the same session.
Furthermore, a collection may be saved to and loaded from a file.
This allows us to generate a index once and then load it from disk, which is much faster then regenerating it each time we use it:

.. code-block:: python

    with Collection.open('index.txt') as index:
        if update_index:
            index.update(signac.index())
        docs = index.find({'statepoint.a': 42})

Since **signac**'s decentralized approach is not designed to automatically keep track of changes, it is up to the user to determine when a particular index needs to be updated.
To automatically identify and remove stale documents [#f3]_, use the :py:func:`signac.export` function:


.. code-block:: python

    with Collection.open('index.txt') as index:
        signac.export(signac.index(), index, update=True)

.. [#f3] A *stale* document is associated with a file or state point that has been removed.

.. tip::

    The :py:class:`.Collection` class has the same interface as a :py:class:`pymongo.collection.Collection` class.
    That means you can use these two types of collections interchangeably.

Fetching Data
=============

Index documents can be used to directly fetch associated data.
The :py:func:`signac.fetch` function is essentially equivalent to python's built-in :py:func:`open` function, but instead of a file path it uses an index document [#f1]_ to locate and open the file.

.. code-block:: python

    # Search for specific documents:
    for doc in index.find({'statepoint.a': 42, 'format': 'TextFile'}):
        with signac.fetch(doc) as file:
            do_something_with_file(file)

The :py:func:`~signac.fetch` function will attempt to retrieve data from more than one source if data was :ref:`mirrored <data_mirroring>`.
Overall, this enables us to operate on indexed project data in a way which is more agnostic to its actual source.

.. [#f1] or a file id

.. _access-module:

The *signac_access.py* Module
=============================

We can use the ``signac_access.py`` module to control the index generation across projects.
An **empty** module is equivalent to a module which contains the following directives:

.. code-block:: python

    import signac

    def get_indexes(root):
        yield signac.get_project(root).index()

This means that any index yielded from a ``get_indexes()`` function defined within the access module will be compiled into the master index.

By putting this code explicitly into the module, we have full control over the index generation.
For example, to index all files with a ``.txt`` filename suffix, we would put the following code into the module:

.. code-block:: python

    import signac

    def get_indexes(root):
        yield signac.get_project(root).index(formats='.*\.txt')

You can generate a basic access module for a **signac** project using the :py:meth:`~.Project.create_access_module` method.



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

    class MyCrawler(signac.RegexFileCrawler):
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
