.. _projects:

========
Projects
========

Introduction
============

A **signac** project is a conceptual entity consisting of three components:

  1. a **data space**,
  2. **scripts and routines** that operate on that space, and
  3. the project's **documentation**.

This division corresponds largely to the definition of a computational project outlined by `Wilson et al.`_
The primary function of **signac** is to provide a single interface between component **(2)**, the scripts encapsulating the project logic, and component **(1)**, the underlying data generated and manipulated by these operations.
By maintaining a clearly defined data space that can be easily indexed, **signac** can provide a consistent, homogeneous data access mechanism.
In the process, **signac**'s maintainance of the data space also effectively functions as an implicit part of component **(3)**, the project's documentation.

.. _`Wilson et al.`: https://arxiv.org/abs/1609.00037

**I think we can remove these two lines**
            Larger, more complex computational investigations usually demand a division into multiple subprojects; however there is no simple answer to how exactly to divide routines and data space among individual subprojects.
            A general rule of thumb is that if two projects share more than 50\% of their routines and the data they operate on, they are probably the same project.

.. _project-initialization:

Project Initialization
======================

In order to use **signac** to manage a project's data, the project must be **initialized** as a **signac** project.
After a project has been initialized in **signac**, all shell and Python scripts executed within or below the project's root directory have access to **signac**'s central facility, the **signac** project interface.
The project interface is a **signac** construct that provides simple and consistent access to the project's underlying *data space*. [#f1]_

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

The project data space is stored in the *workspace directory* (which is also named *workspace* by default).
Once a project has been initialized, any data inserted into the data space will be stored within this directory.
This association is not permanent; a project can be reassociated with a new workspace at any time, and it may at times be beneficial to maintain multiple separate workspaces for a single project.
You can access your signac :class:`~.contrib.Project` and the associated *data space* from within your project's root directory or any subdirectory from the command line:

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

The central assumption of the **signac** data model is that the *data space* is divisible into individual data points, consisting of data and metadata, which are uniquely addressable in some manner.
In the context of **signac**, each data point is called a *job*, and its unique address is referred to as a *state point*.
A job can consist of anything data, ranging from a single value to multiple terabytes of simulation data; **signac**'s only requirement is that this data can be encoded in a file.

.. _project-job-statepoints:

State Points
------------

A *state point* is a simply mapping of key-value pairs containing metadata describing the job.
The state point is then used to compute a hash value, called the *job id*, which serves as the unique id for the job.
The **signac** framework keeps track of all data and metadata by associating each job with a *workspace directory*, which is just a subdirectory of the project workspace.
This subdirectory is named by the *job id*, therefore guaranteeing a unique file system path for each *job* within the project's *workspace* directory.

.. note::

    Note that it is illegal for multiple jobs to have the same state point. In **signac**, the state point must be unique.

Both the state point and the job id are equivalent addresses for jobs in the data space.
To access or modify a data point, obtain an instance of :py:class:`~.Project.Job` by passing the associated metadata as a mapping of key-value pairs (for example, as an instance of :py:class:`dict`) into the :py:meth:`~.Project.open_job` method.

.. code-block:: python

    # Define a state point:
    >>> statepoint = {'a': 0}
    # Get the associated job:
    >>> job = project.open_job(statepoint)
    >>> print(job.get_id())
    9bfd29df07674bc4aa960cf661b5acd2
    >>> job.init()
    # The job already exists
    >>> job in project
    True
    >>> job2 = project.open_job(id=job.get_id())
    >>> job == job2
    True

Note that the job above already existed in the project.
In general, the ``job`` instance only gives you a handle to a python object; to create the underlying workspace directory, you must *initialize* it.
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

Note that when first creating a job, its job id has not yet been computed, so calling ``project.open_job`` with an `id` argument will fail.

However you opened the job, via state point or id, an instance of :py:class:`~.Project.Job` can always be used to retrieve the associated *state point*, the *job id*, and the *workspace* directory with the :py:meth:`~.Project.Job.statepoint` method, the :py:meth:`~.Project.Job.get_id` method, and the :py:meth:`~.Project.Job.workspace` method:

.. code-block:: python

    >>> print(job.statepoint())
    {'a': 0}
    >>> print(job.get_id())
    9bfd29df07674bc4aa960cf661b5acd2
    >>> print(job.workspace())
    '/home/johndoe/my_project/workspace/9bfd29df07674bc4aa960cf661b5acd2'

Evidently, the job's workspace directory is a subdirectory of the workspace whose name is simply some (seemingly arbitrary) string of characters, which is deterministically computed from the state point.

For convenience, the *state point* may also be accessed via the :py:attr:`~.Project.Job.statepoint` or :py:attr:`~.Project.Job.sp` attributes, e.g., the value for ``a`` can be printed using either ``print(job.sp.a)`` or ``print(job.statepoint.a)``.
This also works for **nested** *state points*: ``print(job.sp.b.c)``!
An additional advantage of accessing the statepoint via the attributes is that these can be directly modified, triggering a recalculation of the job id and a renaming of the job's workspace directory.

.. _project-job-statepoint-modify:

Modifying the State Point
^^^^^^^^^^^^^^^^^^^^^^^^^

It may be necessary to change the state point of one or more jobs after initialization--for example, to add previously not needed state point values.
Modifying a state point entails modifying the job id which means that the state point file needs to be rewritten and the job's workspace directory is renamed, both of which are computationally cheap operations.
The user is nevertheless advised **to take great care when modifying a job's state point** since errors may render the data space **inconsistent**.

There are three main options for modifying a job's state point:

    1. Directly via the job's :py:attr:`~.Project.Job.statepoint` and :py:attr:`~.Project.Job.sp` attributes,
    2. via the job's :py:meth:`~.Project.Job.update_statepoint` method, and
    3. via the job's :py:meth:`~.Project.Job.reset_statepoint` method.

The :py:meth:`~.Project.Job.update_statepoint` method provides safe-guards against accidental overwriting of existing *state point* values, while :py:meth:`~.Project.Job.reset_statepoint` will simply reset the whole *state point* without further questions.
The :py:attr:`~.Project.Job.statepoint` and :py:attr:`~.Project.Job.sp` attributes provide the greatest flexibility, but similar to :py:meth:`~.Project.Job.reset_statepoint` they provide no additional protection.

.. important::

    Regardless of method, **signac** will always raise a :py:class:`~.errors.DestinationExistsError` if a *state point* modification would result in the overwriting of an existing job.


The following examples demonstrate how to **add**, **rename** and **delete** *state point* keys using the :py:attr:`~.Project.Job.sp` attribute:

To **add a new key** ``b`` to all existing *state points*, execute:

.. code-block:: python

    for job in project:
        if 'b' not in job.sp:
            job.sp.b = 0

**Renaming** a state point key from ``b`` to ``c``:

.. code-block:: python

    for job in project:
        if 'c' not in job.sp:
            job.sp.c = job.sp.pop('b')

To **remove** a state point key ``c``:

.. code-block:: python

    for job in project:
        try:
            del job.sp['c']
        except KeyError:
            pass  # already deleted

You can modify **nested** *state points* in-place, but you will need to use dictionaries to add new nested keys, e.g.:

.. code-block:: python

    >>> job.statepoint()
    {'a': 0}
    >>> job.sp.b.c = 0  # <-- will raise a KeyError!!

    # Instead:
    >>> job.sp.b = {'c': 0}

    # Now you can modify in-place:
    >>> job.sp.b.c = 1

.. _project-job-document:

The Job Document
----------------

In addition to the state point, additional metadata can be associated with your job in the form of simple key-value pairs using the job :py:attr:`~.Job.document`!
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
  2) keeping track of **runtime information** or to
  3) **label** jobs, e.g. to identify error states.

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

However, the `~.Project.find_jobs` interface is much more powerful in that it allows filtering for subsets of jobs.
For example, to iterate over all jobs that have a *state point* parameter ``b=0``, execute:

.. code-block:: python

    for job in project.find_jobs({'b': 0}):
        pass

For more information on how to search for specific jobs in Python and on the command line, please see the :ref:`query` chapter.

.. _project-job-grouping:

Grouping
--------

**To be written**

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

To **permanently delete** a job and its contents use the :py:meth:`~.Project.Job.remove` method:

.. code-block:: python

    job = project.open_job(statepoint)
    job.remove()
    assert job not in project


Schema Detection
================

**To be written**


.. _data-space-operations:

Data Space Operations
=====================

It is highly recommended to divide individual modifications of your project's data space into distinct functions.
In this context, a *data space operation* is defined as a function with the primary argument being an instance of :py:class:`~.Project.Job`.

That means, the initialization of a *job*, either implicitly or explicitly, is always the first data space operation.
For demonstration purposes we are going to initialize a data space with two numbers ``a`` and ``b`` from 0 to 25, calculate the product of these two numbers and store the result in a file called ``product.txt``.

First, we define our product function:

.. code-block:: python

    def compute_product(job):
        with job:
            with open('product.txt', 'w') as file:
                file.write(str(job.sp.a * job.sp.b))

In this example we use the job as `context manager`_ to switch into the job's *workspace* directory.
Then we access the two numbers ``a`` and ``b`` via the :py:attr:`~.Project.Job.sp` *state point* interface and write their product to a file called ``product.txt`` located within the job's *workspace*.
Alternatively, we could also store the result in the :ref:`job document <project-job-document>`:

.. code-block:: python

    def compute_product(job):
        job.document['product'] = job.sp.a * job.sp.b

.. _`context manager`: http://effbot.org/zone/python-with-statement.htm

Next, we are going to initialize the project's *data space* by iterating over the two numbers, obtaining the :py:class:`~.Project.Job` instance with :py:meth:`~.Project.open_job` and calling the :py:meth:`~.Project.Job.init` method:

.. code-block:: python

    project = signac.get_project()
    for i in range(25):
        for j in range(25):
            job = project.open_job({'a': i, 'b': j})
            job.init()

We can then execute our operation for the complete data space, for example, like this:

.. code-block:: python

    for job in project:
        compute_product(job)

Finally, we can now retrieve our pre-calculated products by defining an access function,

.. code-block:: python

    def product(a, b):
        job = project.open_job({'a': a, 'b': b}):
        with open(job.fn('product.txt')) as file:
            return int(file.read())

Here, we first retrieve the corresponding job to our input values and then return the result using the :py:meth:`~.Project.Job.fn` convenience method, where ``job.fn(filename)`` is equivalent to  ``os.path.join(job.workspace(), filename)``.

.. note::

    In reality, we should account for missing values, for example, by catching :py:class:`FileNotFoundError` exceptions, by checking whether the job is actually part of our data space with ``job in project`` or using the :py:meth:`~.Project.Job.isfile` method (or any combination thereof).

Parallelization
---------------

To execute a :ref:`data space operation <data-space-operations>` ``func()`` for the complete :ref:`project data space <project-data-space>` in serial we can either run a for-loop as shown before:

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

Workspace Views
===============

The workspace structure is organized by job id, which is efficient and flexible for organizing the data.
However, inspecting files as part of a job workspace directly on the file system is now harder.

In this case it is useful to create a *linked view*, that means, a directory hierarchy with human-readable
names, that link to the actual job workspace directories.
This means that no data is copied, but you can inspect data in a more convenient way.

To create a linked view you can either call the :py:meth:`~.Project.create_linked_view` method or execute
the ``signac view`` function on the command line.

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

As the parameter *b* is constant for all jobs within the data space, it is ignored for the creation of the linked views.

.. important::

    When the project data space is changed by adding or removing jobs, simply update the view, by executing :py:meth:`~.Project.create_linked_view` or ``signac view`` for the same view directory again.

You can limit the *linked view* to a specific data subset by providing a set of *job ids* to the :py:meth:`~.Project.create_linked_view` method.
This works similar for ``$ signac view`` on the command line, for example, in combination with ``signac find``:

.. code-block:: bash

    $ signac find '{"a": 0}' | xargs signac view my_view -j

.. tip::

    Consider creating a linked view for large data sets on an in-memory file system for best performance.

.. _move-copy-remove:

Centralized Data
================

**To be written**

Synchronization
================

**To be written**
