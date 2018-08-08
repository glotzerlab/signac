.. _projects:

========
Projects
========

Introduction
============

For a full reference of the Project API, please see the :ref:`Python API
<python-api-project>`.

A **signac** project is a conceptual entity consisting of three components:

  1. a **data space**,
  2. **scripts and routines** that operate on that space, and
  3. the project's **documentation**.

This division corresponds largely to the definition of a computational project outlined by `Wilson et al.`_
The primary function of **signac** is to provide a single interface between component **(2)**, the scripts encapsulating the project logic, and component **(1)**, the underlying data generated and manipulated by these operations.
By maintaining a clearly defined data space that can be easily indexed, **signac** can provide a consistent, homogeneous data access mechanism.
In the process, **signac**'s maintainance of the data space also effectively functions as an implicit part of component **(3)**, the project's documentation.

.. _`Wilson et al.`: https://arxiv.org/abs/1609.00037

.. _project-initialization:

Project Initialization
======================

In order to use **signac** to manage a project's data, the project must be **initialized** as a **signac** project.
After a project has been initialized in **signac**, all shell and Python scripts executed within or below the project's root directory have access to **signac**'s central facility, the **signac** project interface.
The project interface provides simple and consistent access to the project's underlying *data space*. [#f1]_

.. [#f1] You can access a project interface from other locations by explicitly specifying the root directory.

To initialize a project, simply execute ``$signac init <projectname>`` on the command line inside the desired project directory (create a new project directory if needed).
For example, to initialize a **signac** project named *MyProject* in a directory called ``my_project``, execute:

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject

You can alternatively initialize your project within Python with the :py:func:`~.init_project` function:

.. code-block:: python

    >>> project = signac.init_project('MyProject')

This will create a configuration file which contains the name of the project.
The directory that contains this configuration file is the project's root directory.

.. _project-data-space:

The Data Space
==============

The project data space is stored in the *workspace directory*.
By default this is a sub-directory within the project's root directory named *workspace*.
Once a project has been initialized, any data inserted into the data space will be stored within this directory.
This association is not permanent; a project can be reassociated with a new workspace at any time, and it may at times be beneficial to maintain multiple separate workspaces for a single project.
You can access your signac :py:class:`~.Project` and the associated *data space* from within your project's root directory or any subdirectory from the command line:

.. code-block:: shell

    $ signac project
    MyProject

Or with the :py:func:`~signac.get_project` function:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> print(project)
    MyProject

.. image:: images/signac_data_space.png

.. _project-jobs:

Jobs
====

For a full reference of the Job API, please see the :ref:`Python API
<python-api-job>`.

The central assumption of the **signac** data model is that the *data space* is divisible into individual data points, consisting of data and metadata, which are uniquely addressable in some manner.
In the context of **signac**, each data point is called a *job*, and its unique address is referred to as a *state point*.
A job can consist of any type of data, ranging from a single value to multiple terabytes of simulation data; **signac**'s only requirement is that this data can be encoded in a file.

.. _project-job-statepoints:

State Points
------------

A *state point* is a simple mapping of key-value pairs containing metadata describing the job.
The state point is then used to compute a hash value, called the *job id*, which serves as the unique id for the job.
The **signac** framework keeps track of all data and metadata by associating each job with a *workspace directory*, which is just a subdirectory of the project workspace.
This subdirectory is named by the *job id*, therefore guaranteeing a unique file system path for each *job* within the project's *workspace* directory.

.. note::

    Because **signac** assumes that the state point is a unique identifier, multiple jobs cannot share the same state point.
    A typical remedy for scenarios where, *e.g.*, multiple replicas are required, is to append the replica number to the state point to generate a unique state point.

Both the state point and the job id are equivalent addresses for jobs in the data space.
To access or modify a data point, obtain an instance of :py:class:`~.Project.Job` by passing the associated metadata as a mapping of key-value pairs (for example, as an instance of :py:class:`dict`) into the :py:meth:`~.Project.open_job` method.

.. code-block:: python

    # Define a state point:
    >>> statepoint = {'a': 0}
    # Get the associated job:
    >>> job = project.open_job(statepoint)
    >>> print(job.get_id())
    9bfd29df07674bc4aa960cf661b5acd2


In general an instance of :py:class:`~.Project.Job` only gives you a handle to a python object.
To create the underlying workspace directory and thus make the job part of the data space, you must *initialize* it.
You can initialize a job **explicitly**, by calling the :py:meth:`~.Project.Job.init` method, or **implictly**, by either accessing the job's :ref:`job document <project-job-document>` or by switching into the job's workspace directory.

.. code-block:: python

    >>> job = project.open_job({'a': 2})
    # Job does not exist yet
    >>> job in project
    False
    >>> job.init()
    # Job now exists
    >>> job in project
    True

Once a job has been initialized, it may also be *opened by id* as follows (initialization is required because prior to initialization the job id has not yet been calculated):

.. code-block:: python

    >>> job.init()
    >>> job2 = project.open_job(id=job.get_id())
    >>> job == job2
    True

Whether a job is opened by state point or job id, an instance of :py:class:`~.Project.Job` can always be used to retrieve the associated *state point*, the *job id*, and the *workspace* directory with the :py:meth:`~.Project.Job.statepoint`, :py:meth:`~.Project.Job.get_id`, and :py:meth:`~.Project.Job.workspace` methods, respectively:

.. code-block:: python

    >>> print(job.statepoint())
    {'a': 0}
    >>> print(job.get_id())
    9bfd29df07674bc4aa960cf661b5acd2
    >>> print(job.workspace())
    '/home/johndoe/my_project/workspace/9bfd29df07674bc4aa960cf661b5acd2'

Evidently, the job's workspace directory is a subdirectory of the project's workspace and is named by the job's id.
We can use the :py:meth:`.Job.fn` convenience function to prepend the this workspace path to a file name; ``job.fn(filename)`` is equivalent to ``os.path.join(job.workspace(), filename)``.
This function makes it easy to create or open files which are associated with the job:

.. code-block:: python

    >>> print(job.fn('newfile.txt'))
    '/home/johndoe/my_project/workspace/9bfd29df07674bc4aa960cf661b5acd2/newfile.txt'

For convenience, the *state point* may also be accessed via the :py:attr:`~.Project.Job.statepoint` or :py:attr:`~.Project.Job.sp` attributes, e.g., the value for ``a`` can be printed using either ``print(job.sp.a)`` or ``print(job.statepoint.a)``.
This also works for **nested** *state points*: ``print(job.sp.b.c)``!
An additional advantage of accessing the statepoint via the attributes is that these can be directly modified, triggering a recalculation of the job id and a renaming of the job's workspace directory.

.. _project-job-statepoint-modify:

Modifying the State Point
^^^^^^^^^^^^^^^^^^^^^^^^^

As just mentioned, the state point of a job can be changed after initialization.
A typical example where this may be necessary, is to add previously not needed state point keys.
Modifying a state point entails modifying the job id which means that the state point file needs to be rewritten and the job's workspace directory is renamed, both of which are computationally cheap operations.
The user is nevertheless advised **to take great care when modifying a job's state point** since errors may render the data space **inconsistent**.

There are three main options for modifying a job's state point:

    1. Directly via the job's :py:attr:`~.Project.Job.statepoint` and :py:attr:`~.Project.Job.sp` attributes,
    2. via the job's :py:meth:`~.Project.Job.update_statepoint` method, and
    3. via the job's :py:meth:`~.Project.Job.reset_statepoint` method.

The :py:meth:`~.Project.Job.update_statepoint` method provides safeguards against accidental overwriting of existing *state point* values, while :py:meth:`~.Project.Job.reset_statepoint` will simply reset the whole *state point* without further questions.
The :py:attr:`~.Project.Job.statepoint` and :py:attr:`~.Project.Job.sp` attributes provide the greatest flexibility, but similar to :py:meth:`~.Project.Job.reset_statepoint` they provide no additional protection.

.. important::

    Regardless of method, **signac** will always raise a :py:class:`~.errors.DestinationExistsError` if a *state point* modification would result in the overwriting of an existing job.


The following examples demonstrate how to **add**, **rename** and **delete** *state point* keys using the :py:attr:`~.Project.Job.sp` attribute:

To **add a new key** ``b`` to all existing *state points* that do not currently contain this key, execute:

.. code-block:: python

    for job in project:
        if 'b' not in job.statepoint:
            job.sp.b = 0

**Renaming** a state point key from ``b`` to ``c``:

.. code-block:: python

    for job in project:
        if 'c' not in job.statepoint:
            job.sp.c = job.statepoint.pop('b')

To **remove** a state point key ``c``:

.. code-block:: python

    for job in project:
        try:
            del job.statepoint['c']
        except KeyError:
            pass  # already deleted

You can modify **nested** *state points* in-place, but you will need to use dictionaries to add new nested keys, e.g.:

.. code-block:: python

    >>> job.statepoint()
    {'a': 0}
    >>> job.statepoint.b.c = 0  # <-- will raise an AttributeError!!

    # Instead:
    >>> job.statepoint.b = {'c': 0}

    # Now you can modify in-place:
    >>> job.statepoint.b.c = 1

.. _project-job-document:

The Job Document
----------------

In addition to the state point, additional metadata can be associated with your job in the form of simple key-value pairs using the job :py:attr:`~.Job.document`.
This *job document* is automatically stored in the job's workspace directory in `JSON`_ format.

.. _`JSON`: https://en.wikipedia.org/wiki/JSON

.. code-block:: python

    >>> job = project.open_job(statepoint)
    >>> job.document['hello'] = 'world'

Just like the job *state point*, individual keys may be accessed either as attributes or through a functional interface, *e.g.*:

.. code-block:: python

    >>> print(job.document().get('hello'))
    world
    >>> print(job.document.hello)
    world
    >>> print(job.doc.hello)
    world

.. tip::

     Use the :py:meth:`Job.document.get` method to return ``None`` or another specified default value for missing values. This works exactly like with python's `built-in dictionaries <https://docs.python.org/3/library/stdtypes.html#dict.get>`_.

Use cases for the **job document** include, but are not limited to:

  1) **storage** of *lightweight* data,
  2) Tracking of **runtime information**
  3) **labeling** of jobs, e.g. to identify error states.

.. _project-job-finding:

Finding jobs
------------

In general, you can iterate over all initialized jobs using the following idiom:

.. code-block:: python

    for job in project:
        pass

This notation is shorthand for the following snippet of code using the :py:meth:`~.Project.find_jobs` method:

.. code-block:: python

    for job in project.find_jobs():
        pass

However, the :py:meth:`~.Project.find_jobs` interface is much more powerful in that it allows filtering for subsets of jobs.
For example, to iterate over all jobs that have a *state point* parameter ``b=0``, execute:

.. code-block:: python

    for job in project.find_jobs({'b': 0}):
        pass

For more information on how to search for specific jobs in Python and on the command line, please see the :ref:`query` chapter.

.. _project-job-grouping:

Grouping
--------

Grouping operations can be performed on the complete project data space or the results of search queries, enabling aggregated analysis of multiple jobs and state points.

The return value of the :py:meth:`.Project.find_jobs()` method is an iterator over all jobs (or all jobs matching an optional filter if one is specified).
This iterator is an instance of :py:class:`~.contrib.project.JobsCursor` and allows us to group these jobs by state point parameters, the job document values, or even arbitrary functions.

.. note::

    The :py:meth:`~.Project.groupby` method is very similar to Python's built-in :py:func:`itertools.groupby` function.


Basic Grouping by Key
^^^^^^^^^^^^^^^^^^^^^

Grouping can be quickly performed using a statepoint or job document key.

If *a* was a state point variable in a project's parameter space, we can quickly enumerate the groups corresponding to each value of *a* like this:

.. code-block:: python

    for a, group in project.groupby('a'):
        print(a, list(group))

Similarly, we can group by values in the job document as well. Here, we group all jobs in the project by a job document key *b*:

.. code-block:: python

    for b, group in project.groupbydoc('b'):
        print(b, list(group))


Grouping by Multiple Keys
^^^^^^^^^^^^^^^^^^^^^^^^^

Grouping by multiple state point parameters or job document values is possible, by passing an iterable of fields that should be used for grouping.
For example, we can group jobs by state point parameters *c* and *d*:

.. code-blocK:: python

    for (c, d), group in project.groupby(('c', 'd')):
        print(c, d, list(group))


Searching and Grouping
^^^^^^^^^^^^^^^^^^^^^^

We can group a data subspace by combining a search with a group-by function.
As an example, we can first select all jobs, where the state point key *e* is equal to 1 and then group them by the state point parameter *f*:

.. code-block:: python

    for f, group in project.find_jobs({'e': 1}).groupby('f'):
        print(f, list(group))


Custom Grouping Functions
^^^^^^^^^^^^^^^^^^^^^^^^^

We can group jobs by essentially arbitrary functions.
For this, we define a function that expects one argument and then pass it into the :py:meth:`~.Project.groupby` method.
Here is an example using an anonymous *lambda* function as the grouping function:

.. code-block:: python

    for (d, count), group in project.groupby(lambda job: (job.sp['d'], job.document['count'])):
        print(d, count, list(group))


.. _project-job-move-copy-remove:

Moving, Copying and Removal
---------------------------

In some cases it may desirable to divide or merge a project data space.
To **move** a job to a different project, use the :py:meth:`~.Project.Job.move` method:

.. code-block:: python

    other_project = get_project(root='/path/to/other_project')

    for job in jobs_to_move:
        job.move(other_project)

**Copy** a job from a different project with the :py:meth:`~.Project.clone` method:

.. code-block:: python

    project = get_project()

    for job in jobs_to_copy:
        project.clone(job)

Trying to move or copy a job to a project which has already an initialized job with the same *state point*, will trigger a :py:class:`~.errors.DestinationExistsError`.

.. warning::

    While **moving** is a cheap renaming operation, **copying** may be much more expensive since all of the job's data will be copied from one workspace into the other.

To **clear** all data associated with a specific job, call the :py:meth:`~.Project.Job.clear` method.
Note that this function will do nothing if the job is uninitialized; the :py:meth:`~.Project.Job.reset` method will also clear all data associated with a job, but it will also automatically initialize the job if it was not originally initialized.
To **permanently delete** a job and its contents use the :py:meth:`~.Project.Job.remove` method:

.. code-block:: python

    job = project.open_job(statepoint)
    job.remove()
    assert job not in project

.. _project-data:

Centralized Project Data
========================

To support the centralization of project-level data, **signac** offers simple facilities for placing data at the project level instead of associating it with a specific job.
For one, **signac** provides a *project document* analogous to the :ref:`job document <project-job-document>`.
The project document is stored in JSON format in the project root directory and can be used to store similar types of data to the job document.

.. code-block:: python

    >>> project = signac.get_project()
    >>> project.document['hello'] = 'world'
    >>> print(project.document().get('hello'))
    'world'
    >>> print(project.document.hello)
    'world'

In addition, **signac** also provides the :py:meth:`.Project.fn` method, which is analogous to the :py:meth:`.Job.fn` method described above:

.. code-block:: python

    >>> print(project.root_directory())
    '/home/johndoe/my_project/'
    >>> print(project.fn('foo.bar'))
    '/home/johndoe/my_project/foo.bar'

.. _schema-detection:

Schema Detection
================

While **signac** does not require you to specify an *explicit* state point schema, it is always possible to deduce an *implicit* semi-structured schema from a project's data space.
This schema is comprised of the set of all keys present in all state points, as well as the range of values that these keys are associated with.

Assuming that we initialize our data space with two state point keys, ``a`` and ``b``, where ``a`` is associated with some set of numbers and ``b`` contains a boolean value:

.. code-block:: python

    for a in range(3):
        for b in (True, False):
            project.open_job({'a': a, 'b': b}).init()


Then we can use the :py:meth:`.Project.detect_schema` method to get a basic summary of keys within the project's data space and their respective range:

.. code-block:: python

    >>> print(project.detect_schema())
    {
     'a': 'int([0, 1, 2], 3)',
     'b': 'bool([False, True], 2)',
    }

This functionality is also available directly from the command line:

.. code-block:: bash

    $ signac schema
    {
     'a': 'int([0, 1, 2], 3)',
     'b': 'bool([False, True], 2)',
    }


.. _data-space-operations:

Data Space Operations
=====================

A central goal of maintaining a **signac** data space is to ease the process of operating on this data.
While **signac**'s flexibility enables multiple paradigms of data access and modification, in order to maintain well-defined and clearly segmented workflow it is highly recommended to divide individual modifications of your project's data space into distinct functions.
With this in mind, we define a *data space operation* as a function whose primary argument is an instance of :py:class:`~.Project.Job`.
In this context, the initialization of a *job* is always the first data space operation.

To demonstrate this concept, we initialize a data space with two numbers ``a`` and ``b`` from 0 to 25, calculate the product of these two numbers, and then store the result in a file called ``product.txt``.
First, we define our primary data space operation, the product function:

.. code-block:: python

    def compute_product(job):
        with job:
            with open('product.txt', 'w') as file:
                file.write(str(job.sp.a * job.sp.b))

In this example, we use the job as `context manager`_ to switch into the job's *workspace* directory.
Then, we access the two numbers ``a`` and ``b`` and write their product to a file called ``product.txt`` located within the job's *workspace*.
Alternatively, we could also store the result in the :ref:`job document <project-job-document>`:

.. code-block:: python

    def compute_product(job):
        job.document['product'] = job.sp.a * job.sp.b

.. _`context manager`: http://effbot.org/zone/python-with-statement.htm

Next, we are going to initialize the project's *data space* by iterating over the two numbers, obtaining the :py:class:`~.Project.Job` instance with :py:meth:`~.Project.open_job`, and calling the :py:meth:`~.Project.Job.init` method:

.. code-block:: python

    project = signac.get_project()
    for i in range(25):
        for j in range(25):
            job = project.open_job({'a': i, 'b': j})
            job.init()

We can then execute our operation on the complete data space like so:

.. code-block:: python

    for job in project:
        compute_product(job)

Finally, we can retrieve these products by defining an access function,

.. code-block:: python

    def product(a, b):
        job = project.open_job({'a': a, 'b': b}):
        with open(job.fn('product.txt')) as file:
            return int(file.read())

Here, first we retrieve the job corresponding to our input values and then we return the result using the :py:meth:`~.Project.Job.fn` convenience method.

.. note::

    In reality, we should account for missing values.
    This check could be accomplished by, for example, catching :py:class:`FileNotFoundError` exceptions, checking whether the job is part of our data space with ``job in project``, or by using the :py:meth:`~.Project.Job.isfile` method (or any combination thereof).

Parallelization
---------------

To execute a :ref:`data space operation <data-space-operations>` ``func()`` for the complete :ref:`project data space <project-data-space>` in serial we can either run a for loop as shown before:

.. code-block:: python

    for job in project:
        func(job)

or take advantage of python's built-in :py:func:`map` function for a more concise expression:

.. code-block:: python

    list(map(func, project))

Of course, this also works for a data subspace: ``list(map(func, project.find_jobs(a_filter)))``.

Using the ``map()`` function makes it trivial to implement parallelization patterns, for example, using a process :py:class:`~multiprocessing.pool.Pool`:

.. code-block:: python

    from multiprocessing import Pool

    with Pool() as pool:
        pool.map(func, project)

This will execute ``func()`` for the complete project *data space* on as many processing units as there are available.

.. tip::

    Visualize execution progress with a progress bar by wrapping iterables with tqdm_:

    .. code-block:: python

        from tqdm import tqdm

        map(func, tqdm(project))

.. _tqdm: https://github.com/tqdm/tqdm

We can use the exact same pattern to parallelize using **threads**:

.. code-block:: python

    from multiprocessing.pool import ThreadPool

    with ThreadPool() as pool:
        pool.map(func, project)

Or even with `Open MPI`_ using a :py:class:`~.contrib.mpipool.MPIPool`:

.. _`Open MPI`: https://www.open-mpi.org

.. _`MPIPool`: https://github.com/adrn/mpipool

.. code-block:: python

    from signac.contrib.mpipool import MPIPool

    with MPIPool() as pool:
        pool.map(func, tqdm(project))

.. warning::

    Make sure to execute write-operations only on one MPI rank, e.g.:

    .. code-block:: python

      if comm.Get_rank() == 0:
          job.document['a'] = 0
      comm.Barrier()


.. note::

    Without further knowledge about the exact nature of the data space operation, it is not possible to predict which parallelization method is most efficient.
    The best way to find out is to run a few benchmarks.

.. _workspace-views:

Workspace Views
===============

Workspace organization by job id is both efficient and flexible, but the obfuscation introduced by the job id makes inspecting the workspace directly much harder.
In this case it is useful to create a *linked view*.
In **signac**, a view is simply a directory hierarchy with human-readable names that link to the actual job workspace directories.
The use of links ensures that no data is copied, but the human-readable naming conventions ensure that data can be inspected more easily.

To create a linked view you can either call the :py:meth:`~.Project.create_linked_view` method or execute
``signac view`` on the command line.

Let's assume the data space contains the following *state points*:

    * a=0, b=0
    * a=1, b=0
    * a=2, b=0
    * ...,

where *b* is **constant** for all state points.

We then create the linked view with:

.. code-block:: bash

    $ signac view my_view
    Indexing project...
    $ ls my_view/
    a_0 a_1 a_2 ...

We see that the view directories are named according to state point keys and their corresponding values.
Note that in this case the parameter *b* is ignored for the creation of the linked views because it is constant for all jobs within the data space.

.. important::

    When the project data space is changed by adding or removing jobs, simply update the view, by executing :py:meth:`~.Project.create_linked_view` or ``signac view`` for the same view directory again.

You can limit the *linked view* to a specific data subset by providing a set of *job ids* to the :py:meth:`~.Project.create_linked_view` method.
This works similar for ``$ signac view`` on the command line, for example, in combination with ``signac find`` (using the `-j` option to explicitly specify which jobs to include in the view):

.. code-block:: bash

    $ signac find '{"a": 0}' | xargs signac view my_view -j

.. tip::

    Consider creating a linked view for large data sets on an in-memory file system for best performance.

.. _synchronization:

Synchronization
================

In some cases it may be necessary to store a project at more than one location, perhaps for backup purposes or for remote execution of data space operations.
In this case there will be a regular need to synchronize these data spaces.

Synchronization of two projects can be accomplished by either using ``rsync`` to directly synchronize the respective workspace directories, or by using ``signac sync``, a tool designed for more fine-grained synchronization of project data spaces.
Users who are familiar with ``rsync`` will recognize that most of the core functionality and API of ``rsync`` is replicated in ``signac sync``.

As an example, let's assume that we have a project stored locally in the path ``/data/my_project`` and want to synchronize it with ``/remote/my_project``.
We would first change into the root directory of the project that we want to synchronize data into.
Then we would call ``signac sync`` with the path of the project that we want to *synchronize with*:

.. code-block:: bash

    $ cd /data/my_projcet
    $ signac sync /remote/my_project

This would copy data *from the remote project to the local project*.
For more details on how to use ``signac sync``, type ``$ signac sync --help``.
