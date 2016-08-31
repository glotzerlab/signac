.. _tutorial_indexing:

==============
Exploring Data
==============

Finding jobs
============

By default, using the :py:meth:`~.Project.find_jobs` function will return a list of all jobs within the workspace:

.. code-block:: python

    >>> for job in project.find_jobs():
    ...     print(job)
    ...
    8629822576debc2bfbeffa56787ca348
    9110d0837ad93ff6b4013bae30091edd
    # ...

Similarly, we can execute ``signac find`` on the command line to get a list of all *job ids* within the workspace:

.. code-block:: bash

    $ signac find
    Indexing project...
    8629822576debc2bfbeffa56787ca348
    9110d0837ad93ff6b4013bae30091edd
    # ...

A standard operation is to find and operate on a **data subset**.
For this purpose we can use a filter argument, which will return all jobs with matching statepoints:

.. code-block:: python
    
    >>> for job in project.find_jobs({'p': 1.0}):
    ...     print(job)
    ...
    ee617ad585a90809947709a7a45dda9a


Or equivalently on the command line:

.. code-block:: bash

    $ signac find '{"p": 0.1}'
    Indexing project...
    5a6c687f7655319db24de59a2336eff8

Next, we verify the selection by piping the output of ``signac find`` into the ``signac statepoints`` command via ``xargs``:

.. code-block:: bash

    $ signac find '{"p": 0.1}' | xargs signac statepoint
    Indexing project...
    {"N": 1000, "p": 0.1, "kT": 1.0}


Instead of filtering by statepoint, we can also filter by values in the *job document* (or both):

.. code-block:: python

    >>> for job in project.find_jobs(doc_filter={'V': 100}):
    ...     print(job)
    ...
    5a456c131b0c5897804a4af8e77df5aa

Finding jobs by certain criteria requires an index of the data space.
In the previous examples this index was created implicitly, however depending on the data space size, it may make sense to create the index explicitly for multiple uses.
This is shown in the next section.


Indexing
========

An index is a complete record of the data and its associated metadata within our project's data space.
To create an index, we need to crawl through the project's data space, for example by calling the :py:meth:`~.Project.index` method:

.. code-block:: python

    >>> for doc in project.index():
    ...     print(doc)
    {'statepoint': {'p': 5.6, 'N': 1000, 'kT': 1.0}, '_id': '05061d2acea19d2d9a25ac3360f70e04', 'signac_id': '05061d2acea19d2d9a25ac3360f70e04', 'V': 178.57142857142858}
    {'statepoint': {'p': 1.2000000000000002, 'N': 1000, 'kT': 1.0}, '_id': '22582e83c6b12336526ed304d4378ff8', 'signac_id': '22582e83c6b12336526ed304d4378ff8', 'V': 833.3333333333333}
    # ...

Or by executing the ``signac index`` function on the command line:

.. code-block:: bash

    $ signac index
    Indexing project...
    {"signac_id": "05061d2acea19d2d9a25ac3360f70e04", "V": 178.57142857142858, "statepoint": {"N": 1000, "p": 5.6, "kT": 1.0}, "_id": "05061d2acea19d2d9a25ac3360f70e04"}
    {"signac_id": "22582e83c6b12336526ed304d4378ff8", "V": 833.3333333333333, "statepoint": {"N": 1000, "p": 1.2000000000000002, "kT": 1.0}, "_id": "22582e83c6b12336526ed304d4378ff8"}
    # ...

We can store and reuse this index, e.g. to speed up find operations:

.. code-block:: bash

    $ signac index > index.txt
    Indexing project...
    $ signac find --index=index.txt
    Reading index from file 'index.txt'...
    05061d2acea19d2d9a25ac3360f70e04
    e8186b9b68e18a82f331d51a7b8c8c15
    # ...

At this point the index contains information about the statepoint and all data stored in the *job document*.
If we used text files to store data we need to additionally specify the format of those file to make them *indexable*.
In general, any python class may be a format definition, however optimally a format class provides a file-like interface.
An example for such a format class is the :py:class:`~.contrib.formats.TextFile` class.
We will specify that in addition to the *job documents* all files named ``V.txt`` within our data space are to be indexed as *TextFiles*:

.. code-block:: python

    # create_index.py
    import signac
    from signac.contrib.formats import TextFile

    project = signac.get_project()
    for doc in project.index({'.*/V\.txt': TextFile}):
        print(doc)

The regular expression ``.*/V\.txt`` specifies that all files ending in ``V.txt`` are to be indexed, that would include sub-directories!

Views
=====

Sometimes we want to examine our data on the file system directly.
However the file paths within the workspace are obfuscated by the *job id*.
The solution is to use *views*, which are human-readable, but maximal compact hierarchical links to our data space.

To create a linked view we simply execute the :py:meth:`~.Project.create_linked_view` method within python or the ``signac view`` command on the command line:

.. code-block:: bash

    $ mkdir my_view
    $ signac view my_view/
    Indexing project...

The directory ``my_view`` now contains links to the data within the workspace:

.. code-block:: bash

     $ ls my_view/
     p_0.1      p_10.0      p_3.4  p_5.6     p_7.8
     p_1.2 p_2.3  p_4.5     p_6.7     p_8.9

.. note::

    The actual file paths will slightly differ because of floating point precision.

This allows us to examine the data with human-readable path names:

.. code-block:: bash

    $ cat my_view/p_0.1/job/V.txt
    10000.0

.. tip::

    Consider creating a linked view for large data sets on an in-memory file system for best performance.

This completes the basic tutorial.
The next section shows how to complete the workflow and make it more flexible.
