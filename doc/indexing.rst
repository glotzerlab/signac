.. _indexing:

========
Indexing
========

Concept
=======

To create a homogeneous data access layer, **signac** encourages the creation of a data index.
The data index contains all information about the project data structure and can be stored in a database or data frame which then allows the execution of query and aggregation operations on the data.

**signac** uses crawlers to `crawl` through a data source to generate an index.
A crawler is defined to generate a sequence of (*id*, *document*) tuples, the data source is arbitrary.
For example, this would be a valid (albeit not useful) crawler:

.. code-block:: python
   
    class MyCrawler(object):
        def crawl(self):
            for i in range(3):
                yield (str(i), {'a': i})

All crawlers defined by **signac** inherit from the abstract base class :py:class:`~signac.contrib.BaseCrawler`.
It is recommended to inherit specializations of crawlers from this class.

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


This regular expression matches all filenames which begin with `a_`, followed by one more digits, ending with `.txt`.
The definition of a named group, `a`, matching only the digits allows the :py:class:`~signac.contrib.crawler.RegexFileCrawler` to extract the meta data from this filename.
For example:

    ``a_0.txt -> {'a': 0}``
    ``a_1.txt -> {'a': 1}``
    ... and so on.

Each pattern is then associated with a specific format through the :py:meth:`~signac.contrib.RegexFileCrawler.define` class method.
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

    MyCrawler(RegexFileCrawler):
        pass

    # This expressions yields mappings of the type: {'a': value_of_a}.
    MyCrawler.define('a_(?P<a>\d+)\.txt', TextFile)

In this case we could also use :class:`.contrib.formats.TextFile`
as data type which is a more complete implementation of the example shown above.

.. _`file-like object`: https://docs.python.org/3/glossary.html#term-file-object

The index is then generated through the :py:meth:`~signac.contrib.BaseCrawler.crawl` method and can be stored in a database collection:

.. code-block:: python

   crawler = MyCrawler('/data/my_project')
   db.index.insert_many(crawler.crawl())

.. hint::

    Use the optimized export functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo` for faster export and avoidance of duplicates.

Indexing a signac project
-------------------------

Indexing signac projects is simplified by using a :py:class:`~signac.contrib.SignacProjectCrawler`.
In this case meta data is automatically retrieved from the state point as well as from the :py:meth:`job.document <signac.contrib.job.Job.document>`.

Using a :py:class:`~signac.contrib.SignacProjectCrawler` we only need to point the crawler at the project's workspace and all state points are automatically retrieved from the state point manifest file.

.. code-block:: python

    import signac
    from signac.contrib.formats import TextFile

    class MyCrawler(signac.contrib.SignacProjectCrawler):
        pass
    MyCrawler.define('.*\.txt', Textfile)

Notice that we used the regular expression to identify the text files that we want to index, but not to identify the state point.
However we can further extend the meta data using regular expressions to further diversify data within the state point data space.
An expression such as ``.*\(?P<class>init|final)\.txt`` will only match files named ``init.txt`` or ``final.txt``, and will add a field ``class`` to the database record, which will either have the value ``init`` or ``final``.


Master crawlers
===============

It is highly recommended to not execute crawlers directly, but rather use a so called :py:class:`~signac.contrib.MasterCrawler`, which tries to find other crawlers and automatically executes them.
Using a :py:class:`~signac.contrib.MasterCrawler` we don't need to care about the actual location of the data within the file system as long as the local hierarchy is preserved.

The *signac_acess.py* module
----------------------------

The master crawler searches for modules called ``signac_access.py`` and tries to call a function called ``get_crawlers()``.
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

The master crawler is then executed for the indexed data space.

.. code-block:: python

    master_crawler = signac.contrib.MasterCrawler('/projects')
    db.index.insert_many(master_crawler.crawl(), depth=1)

.. warning::

    Especially for master crawlers it is recommended to reduce the crawl depth to avoid too extensive crawling operations over the *complete* filesystem.

Examples for *signac_access.py*
-------------------------------

This is a minimal example for a ``signac_access.py`` file using a :py:class:`~signac.contrib.RegexFileCrawler`:

.. code-block:: python

    # signac_access.py
    import os

    import signac
    from signac.contrib.formats import TextFile


    # Define a crawler class for each structure
    MyCrawler(RegexFileCrawler): pass

    # Add file definitions for each file type, that should be part of the index.
    MyCrawler.define('a_(?P<a>\d+\.txt', TextFile)

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

Optimization
------------

When exporting to a database, such as MongoDB it is more efficent to use specialized export functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo`:

.. code-block:: python

    signac.contrib.export_pymongo(master_crawler, db.master_index, depth=1)

The functions :py:func:`~signac.contrib.export` and :py:func:`~signac.contrib.export_pymongo` are optimized for exporting to an index collection, ensuring that the collection does not contain any duplicates.
The behavior of these functions is roughly equivalent to

.. code-block:: python

    for _id, doc in crawler.crawl(*args, **kwargs):
        index.replace_one({'_id': _id}, doc)

Tagging
-------

It may be desirable to only index select projects for a specific index for example to distinguish between public and private indexes.
For this purpose it is possible to provide  a set of tags to any crawler, as such:

.. code-block:: python

    class MyCrawler(SignacProjectCrawler):
        tags = {'public', 'miller'}


Master crawlers will ignore all crawlers with defined tags, that do not match *at least one* tag, e.g.:

.. code-block:: python

    # Any of the following master crawlers would ignore MyCrawler:
    master_crawler.tags = None
    master_crawler.tags = {}
    master_crawler.tags = {'private'}  
    # or any other set of tags that does not contain either 'public' or 'miller' or both.

    # These master crawlers would execute MyCrawler:
    master_crawler.tags = {'public'}
    master_crawler.tags = {'miller'}
    master_crawler.tags = {'miller', 'public'}
    master_crawler.tags = {'private', 'miller'}
