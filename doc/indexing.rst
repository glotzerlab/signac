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
For example, this would be a valid crawler:

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

Indexing a signac project
=========================

To index a signac project we can either use the :py:meth:`~.Project.index` method or specialize a :py:class:`~.indexing.SignacProjectCrawler`.
The index will contain all initialized jobs and the data stored in the corresponding job documents.
In addition, you can define to index specific files using `regular expression patterns`_ like this:

.. code-block:: python

    for doc in project.index(formats={'.*\.txt': 'TextFile'})
        print(doc)

.. _`regular expression patterns`: https://en.wikipedia.org/wiki/Regular_expression

Each index document contains the state point parameters stored under the ``statepoint`` keyword and for files the file path and format metadata.
You can omit the format definition by providing `None`, but it is usually a good idea to have some form of identifier.

We use the regular expression to identify which files to include, and optionally in which format.
Using named groups we can extract more metadata from the file path.
An expression such as ``.*\(?P<class>init|final)\.txt`` will only match files named ``init.txt`` or ``final.txt``, and will add a field ``class`` to the index document, which will either have the value ``init`` or ``final``.

.. hint::

    While possibly, it is generally advisabe to limit the amount of metadata encoded in filenames to the bare minimum.

This is the same example with a :py:class:`~signac.contrib.SignacProjectCrawler`:

.. code-block:: python

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', 'TextFile')

    project = signac.get_project()
    crawler = MyCrawler(project.workspace())
    for doc in crawler.crawl():
        print(doc)


Indexing by file path
=====================

In some cases you may want to index an existing unmananged data space.
If metadata is primarily defined through file paths, you can specialize a :py:class:`~.contrib.indexing.RegexFileCrawler`.

Assuming we wanted to index text files, with a specific naming pattern, which
specifies a parameter `a` via the filename, e.g.:

.. code-block:: bash

    /data/my_project/a_0.txt
    /data/my_project/a_1.txt
    ...

To extract meta data for this filename structure, we use regular expression groups, e.g.:

    ``a_(?P<a>\d+)\.txt``

This regular expression matches all filenames which begin with `a_`, followed by one or more digits, ending with `.txt`.
The definition of a named group, `a`, matching only the digits allows the :py:class:`~signac.contrib.RegexFileCrawler` to extract the meta data from this filename.
For example:

    ``a_0.txt -> {'a': 0}``
    ``a_1.txt -> {'a': 1}``
    ... and so on.

Each pattern can be associated with a specific format through the :py:meth:`~.indexing.RegexFileCrawler.define` class method.

.. code-block:: python

    class MyCrawler(RegexFileCrawler):
        pass

    # This expressions yields mappings of the type: {'a': value_of_a}.
    MyCrawler.define('a_(?P<a>\d+)\.txt', 'TextFile')


Exporting index documents
=========================

The simplest way to export and store a project index is by piping the output of the following script into a file with

.. code-block:: bash

    $ signac index > my_index.txt

This is essentially equivalent to storing the output of the following script:

.. code-block:: python

    # export_index.py
    import json
    import signac

    for doc in signac.get_project().index():
        print(json.dumps(doc))

A data index is required for certain operations such as the selection of data sub spaces.
We can use a stored (i.e. cached) data index to accelerate these operations.
For example the following command will select the data sub space where `a=0`, using the pre-generated index:

.. code-block:: bash

    $ signac find --index my_index.txt '{"a": 0}'

We can use the :py:func:`signac.export` function to export index documents into a document collection:

.. code-block:: python

    signac.export(collection, project.index())

.. note::

    The :py:func:`signac.export` function will automatically delegate to the optimized :py:func:`signac.export_pymongo` function if the index argument is a :py:class:`pymongo.collection.Collection` instance.

Fetching Data
=============

Index documents retrieved from an index collection can be used to fetch associated data.
The :py:func:`signac.fetch` function is essentially equivalent to the python built-in :py:func:`open` command, but instead of a file path it uses an index document [#f1]_ to locate the file.

.. code-block:: python

    # Get a document from the index:
    doc = index.find_one()

    # Fetch and open the file associated with this document:
    with signac.fetch(doc) as file:
        do_something_with_file(file)

The :py:func:`~signac.fetch` function will attempt to retrieve data from more than one source if data was :ref:`exported to a mirror <data_mirroring>`.
Overall, this enables us to operate on indexed project data in a way which is more agnostic to its actual source.

.. [#f1] or a file id

Master crawlers
===============

Concept
-------

A :py:class:`~signac.contrib.MasterCrawler` compiles a master index by combining index documents from other crawlers.
In this context those crawlers are called *slave crawlers*.
Any crawler (including other master crawlers) can be *slave crawlers*.

.. _signac-access:

The master crawler searches for modules called ``signac_access.py`` and tries to call a function called ``get_crawlers()`` defined in those modules.
This function is defined as follows:

.. py:function:: signac_access.get_crawlers(root)
    :noindex:

    Return crawlers to be executed by a master crawler.

    :param root: The path where the access module was found.
    :type root: str
    :returns: A mapping of crawler id and crawler instance.

By putting the crawler definitions from above into a file called *signac_access.py* and adding the ``get_crawlers()`` function, we make those crawlers available to master crawlers:

.. code-block:: python

     # signac_acess.py

     # [crawler definitions as shown before]

     def get_crawlers(root):
        return {'main': MyCrawler(os.path.join(root, 'data'))}

The root argument is the absolute path to the location of the *signac_access.py* file, usually the project's root directory.
The *crawler id*, here ``main``, is a completely arbitrary string, however should not be changed after creating the index.

.. tip::

    Use the :py:meth:`~signac.Project.create_access_module` method to create the access module file for signac projects.

Execution
---------

To compile a master index, simply crawl through a file path that contains `signac_access.py` modules:
Assuming the following directory structure:

.. code-block:: bash

    /projects
      project_a/
        signac_access.py
        ...
      project_b/
        signac_access.py
        ...
      ...

We can compile the master index for all projects with:

.. code-block:: python

    master_crawler = signac.contrib.MasterCrawler('/projects')
    signac.export(master_crawler.crawl(depth=1), index)

It is usually a good idea to reduce the crawl depth for master crawlers, to avoid too extensive crawling operations over the *complete* file system.

Examples for *signac_access.py*
-------------------------------

This is a minimal example for a ``signac_access.py`` file using a :py:class:`~signac.contrib.SignacProjectCrawler`:

.. code-block:: python

    # signac_access.py
    import os

    import signac

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', 'TextFile')

    def get_crawlers(root):
        return {'main': MyCrawler(os.path.join(root, 'path/to/workspace'))}

.. note::

    The root argument for a signac project crawler should be the project's **workspace**.


This is a minimal example for a ``signac_access.py`` file using a :py:class:`~signac.contrib.RegexFileCrawler`:

.. code-block:: python

    # signac_access.py
    import os

    import signac


    # Define a crawler class for each structure
    class MyCrawler(signac.contrib.RegexFileCrawler):
      pass

    # Add file definitions for each file type, that should be part of the index.
    MyCrawler.define('.*/a_(?P<a>\d+)\.txt', 'TextFile')

    # Expose the data structures to a master crawler
    def get_crawlers(root):
      # the crawler id is arbitrary, but should not be changed after index creation
      return {'main': MyCrawler(os.path.join(root, 'my_project'))}


Advanced Indexing
=================

.. sidebar:: Warning: SYSADMIN ZONE

    The following topics are considered *advanced* and most likely only interesting to system administrators.

.. _data_mirroring:

Data mirroring
--------------

Using the :py:func:`signac.fetch` function it is possible retrieve files that are associated with index documents.
Those files will preferably be opened directly via a local system path.
However in some cases it may be desirable to mirror files at a different location, e.g., in a database or a different path to increase the accessibility of files.

Use the mirrors argument in the :py:func:`signac.export` function to automatically mirror all files associated with exported index documents.
**signac** provides handlers for a local file system and the MongoDB `GridFS`_ database file system.

.. code-block:: python

    localfs = fs.LocalFS('/path/to/mirror')
    gridfs = fs.GridFS(db)

    export(crawler.crawl(), index, mirrors=[localfs, gridfs])

.. _`GridFS`: https://docs.mongodb.org/manual/core/gridfs/


To access the data, provide the mirrors argument to the :py:func:`signac.fetch` function:

.. code-block:: python

    for doc in index:
        with signac.fetch(doc, mirrors=[localfs, gridfs]) as file:
            do_something_with_file(file)

.. note::

    File systems are used to fetch data in the order provided, starting
    with the native data path.


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

    master_crawler = MasterCrawler('/path/to/public/projects')

    # We define two mirrors
    public_mirrors = [
      # The GridFS database file system is stored in the
      # same database, that we use to publish the index.
      # This means that anyone who can access the index,
      # will be able to access the associated files.
      signac.fs.GridFS(db),

      # The second mirror is on the local file system.
      # It can be downloaded and made available locally,
      # for example to reduce required network transfers.
      signac.fs.LocalFS('/path/to/mirror')
      ]

    # By defining special tags for projects, which are cleared
    # for publication, we prevent the accidental export of private
    # data to the database.
    master_crawler.tags = {'public'}

    signac.export(master_crawler.crawl(depth=1), db.index, public_mirrors)
