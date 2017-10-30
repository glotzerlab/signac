.. _indexing:

========
Indexing
========

Concept
=======

Data spaces managed with **signac** on the file system are immediately searchable because **signac** creates an index of all relevant files *on the fly* whenever a search operation is executed.
This data index contains all information about the project's files, their location and associated metadata such as the *signac id* and the *state point*.

A file index has *one entry per file* and each document has the following fields:

    * ``id``: a unique value which serves as a primary key
    * ``root``: The root path of the file
    * ``filename``: The filename of the file
    * ``md5``: A MD5-hash value of the file content
    * ``file_id``: A number identifying the file content [#f2]_
    * ``format``: A format definition (optional)

.. [#f2] Identical to the ``md5`` value in the current implementation.

The **signac** project interface is specifically designed to assist with processes related to data curation.
However, especially when working with a data set comprised of multiple projects or sources that are not managed with **signac**, it might be easier to work with a data index directly.

For example, this is how we would access files related to a specific data subset using the project interface:

.. code-block:: python

    for job in project.find_jobs({"a": 42}):
        with open(job.fn('hello.txt')) as file:
            print(file.read())

And this is how we would do the same, but operating directly with an index:

.. code-block:: python

    index = signac.Collection(project.index(".*\.txt"))

    for doc in index.find({
            "statepoint.a": 42,
            "filename": {"$regex": "hello.txt"}}):
        with signac.fetch(doc) as file:
            print(file.read())

Here, we first generate the index with the :py:meth:`.Project.index` function and stored the result in a :py:class:`.Collection` container.
Then, we search the index collection for a specific state point and use :py:func:`.fetch` to open the associated file.
The :py:func:`.fetch` functions works very similar to Python's built-in :py:func:`open` function to open files, but in addition will be able to fetch a file from multiple different sources if necessary.

The next few sections are a more detailed outline of how such a workflow can be realized.

Indexing a signac Project
=========================

As shown in the previous section, a **signac** project index can be generated directly with the :py:meth:`.Project.index` function in Python.
Alternatively, we can generate the index on the command line with ``$ signac project --index``.

A signac project index is like a regular file index, but contains the following additional fields:

  * ``signac_id``: The state point id the document is associated with.
  * ``statepoint``: The state point mapping associated with the file.

Each signac project index will have *at least one* entry for each initialized job.
This special index document is associated with the job's :ref:`document <project-job-document>` file and contains not only the ``signac_id`` and the ``statepoint``, but also the data stored in the job document.
This means the following code snippet would be valid:

.. code-block:: python

    for job in project:
        job.document['foo'] = 'bar'

    for doc in project.index():
        assert doc['foo'] == 'bar'

By default, no additional files are indexed; the user is expected to *explicitly* specify which files should be part of the index as described in the next section.

Indexing files
==============

Indexing specific files as part of a project index requires using regular expressions.
For instance, in the initial example we used the expression ``".*\.txt"`` to specify that all files with a filename ending with ".txt" should be part of the index.

We can extract metadata directly from the filename by using regular expressions with *named groups*.
For example, if we have a filename pattern: ``a_0.txt``, ``a_1.txt`` and so on, where the number following ``a_`` is to be extracted as the ``a`` field, we can use the following expression:

.. code-block:: python

    for doc in project.index('.*a_(?P<a>\d+)'):
        print(doc['a'])

To further simplify the selection of different files from the index, we may provide multiple patterns with an optional *format definition*.
Let's imagine we would like to classify the text files with the ``a`` field from the previous example *as well as* PDF-files that adhere to the following pattern: ``init.pdf`` or ``final.pdf``. This is how we could generate this index:

.. code-block:: python

    formats = {
        '.*a_(?P<a>\d+)\.txt': 'TextFile',
        '.*(?P<class>init|final)\.pdf': 'PDFFile'}

    for doc in project.index(formats):
        print(doc)

.. tip::

    To generate regular expressions for the filename patterns in your data space, copy & paste a few representative filenames into the excellent `regex101`_ online app.
    That will allow you to work out your expressions while getting direct graphical feedback.

.. _`regex101`: https://regex101.com

If we want to file an arbitrary directory structure that is not managed by **signac**, we can use the :py:func:`.index_files` function, that expects the root path as the first argument, and indexes **all files** by default.

.. code-block:: python

    for doc in signac.index_files('/data'):
        pass

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

.. _deep-indexing:

Deep Indexing
=============

We may want to add additional metadata to the index that is neither based on neither the state point, the job document, or the filename, but instead is directly extracted from the data.
Such a pattern is typically referred to as *deep indexing* and can be easily implemented with **signac**.

As an example, imagine that we wanted add the number of lines within a file as an additional metadata field in our data index.
For this, we use Python's built-in :py:func:`map` function, which allows us to apply a function to all index entries:

.. code-block:: python

    def add_num_lines(doc):
        if 'filename' in doc:
            with signac.fetch(doc) as file:
                doc['num_lines'] = len(list(file))
        return doc

    index = map(add_num_lines, project.index())

The ``index`` variable now contains an index, where each index entry has an additional ``num_lines`` field.

.. tip::

    We are free to apply multiple *deep indexing*  functions in succession; the functions are only executed when the ``index`` iterable is actually evaluated.

Searching an Index
==================

An index generated with the :py:meth:`.Project.index` method or any other index function is just an iterable over the index documents.
To be able to **search** the index, we need to either implement routines to select specific documents or use containers that implement such routines, such as the :py:class:`.Collection` class that **signac** uses internally for all search operations.

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

Using loops is a very viable approach as long as the index is not too large and the search queries are relatively simple.
Alternatively, we can manage the index using a :py:class:`.Collection` container, which then allows us to search the index with the query expressions that we are used to elsewhere using **signac**.
For example, to execute the same search operation from above, we could use the :py:meth:`~.Collection.find` method:

.. code-block:: python

    index = Collection(signac.index())

    docs = index.find({'statepoint.a': 42})

.. sidebar:: Tip

    You can search a collection on the command line by calling it's :py:meth:`~.Collection.main` method.

Unless they are very small, searching collections is usually **much more efficient** than the *pure python* approach, especially when searching multiple times within the same session.
Furthermore, since a collection may be saved to and loaded from a file, we only have to generate an index once, saving us the effort of regenerating it each time we use it:

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

Master Indexes
==============

Generating a Master Index
-------------------------

A master index is a compilation of multiple indexes that simplifies operating on a larger data space.
To make a signac project part of a master index, we simply create a file called ``signac_access.py`` in its root directory.
The existance of this file tells **signac** that the projects in those directories should be indexed as part of a master index.

Imagine that we have two projects in two different directories ``~/project_a`` and ``~/project_b`` within our home directory.
We create the ``signac_access.py`` file in each respective project directory like this:

.. code-block:: bash

    $ touch ~/project_a/signac_access.py
    $ touch ~/project_b/signac_access.py

Executing the :py:func:`~.index` function for the home directory

.. code-block:: python

    for doc in signac.index('~'):
        print(doc)

will now yield a joint index for both projects in ``~/project_a`` and ``~/project_b``.

For more information on how to have more control over the index creation, see the :ref:`signac access module <access-module>` section.

.. tip::

  By typing ``$ signac index`` you can directly generate a signac master index on the command line and then pipe it into a file:

  .. code-block:: bash

      $ signac index > index.txt

.. _access-module:

The *signac_access.py* Module
-----------------------------

We can use the ``signac_access.py`` module to control the index generation across projects.
An **empty** module is equivalent to a module which contains the following directives:

.. code-block:: python

    import signac

    def get_indexes(root):
        yield signac.get_project(root).index()

This means that any index yielded from a ``get_indexes()`` function defined within the access module will be compiled into the master index.

By putting this code explicitly into the module, we have full control over the index generation.
For example, to specify that all files with filenames ending with ``.txt`` should be added to the index, we would put the following code into the module:

.. code-block:: python

    import signac

    def get_indexes(root):
        yield signac.get_project(root).index(formats='.*\.txt')

You can generate a basic access module for a **signac** project using the :py:meth:`~.Project.create_access_module` method.

.. tip::

    The ``signac_access.py`` module is perfectly suited to implement `deep indexing <deep-indexing>`_ patterns.

.. _database_integration:

Database Integration
====================


Database access
---------------

After :doc:`configuring <configuration>` one or more database hosts you can access a database with the :py:func:`signac.get_database` function.

.. autofunction:: signac.get_database
    :noindex:

.. _data_mirroring:

Mirroring of Data
-----------------

Using the :py:func:`signac.fetch` function it is possible retrieve files that are associated with index documents.
Those files will preferably be opened directly via a local system path.
However, in some cases it may be desirable to mirror files at a different location, e.g., in a database or a different path, to increase the accessibility of files.

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
----------------------------

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
-----------------------

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
