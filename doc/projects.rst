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

This corresponds largely to the definition of a computational project outlined by `Wilson et al.`_
The signac framework helps to design and implement all three of these components by providing the necessary computational infrastructure and by being an implicit part of the project's documentation.

.. _`Wilson et al.`: https://arxiv.org/abs/1609.00037

Larger, more complex computational investigations usually demand a division into multiple subprojects, however there is no simple answer to how exactly to divide routines and data space among individual subprojects.
A general rule of thumb is that if two projects share more than 50\% of their routines and the data they operate on, they are probably the same project.
Keep in mind that **signac** makes it easy to :ref:`divide or merge <move-copy-remove>` projects even at a later stage.

Project Initialization
======================

To initialize a project, simply create a project directory and execute ``$signac init <projectname>`` on the command line.
For example, to initialize a project named *MyProject* in a directory called ``my_project``, execute:

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

The Project Data Space
======================

After a project has been initialized, all shell and Python scripts executed within or below the project's root directory have access to **signac**'s project interface. [#f1]_
This allows you to access and manipulate the project's *data space* in a simple and consistent manner.

.. [#f1] You can access a project interface from other locations by explicitly specifying the root directory.

The **signac** data models is based on the assumption that the data space may be discretized into individual data points and that each data point is associated with data and metadata.
All data may be stored in form of *files* or in form of *documents*, which are `associative arrays <https://en.wikipedia.org/wiki/Associative_array>`_ stored in JSON-format.
In the context of **signac**, the combination of data and metadata is called a *Job* and each job is uniquely identified through its *state point*.

.. image:: images/signac_data_space.png

With **signac** all files associated with a specific state point are stored within the corresponding job's *workspace* directory.
This allows us to search and access files by their metadata.

You can access a signac :class:`~.contrib.Project` and its *data space* from within your project's root directory or any subdirectory from the command line:

.. code-block:: shell

    $ signac project
    MyProject

Or with the :py:func:`~signac.get_project` function:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> print(project)
    MyProject

Accessing Jobs
==============

Opening Jobs
------------

To access or modify a data point, obtain an instance of :py:class:`~.Project.Job` by passing the associated metadata as a mapping of key-value pairs (for example, as an instance of :py:class:`dict`) into the :py:meth:`~.Project.open_job` method:

.. code-block:: python

    # Define a state point:
    >>> statepoint = {'a': 0}
    # Get the associated job:
    >>> job = project.open_job(statepoint)

To use a particular instance of ``job`` for data storge, add it to the project's data space by *initializing* it, which means to create the corresponding workspace directory:

.. code-block:: python

   >>> job.init()

Once, a job has been initialized it is part of the project's data space and can be retrieved by searching it or by opening it directly *via id*:

.. code-block:: python

    >>> job = project.open_job(id='9bfd29df07674bc4aa960cf661b5acd2')

However you opened the job, via state point or id, an instance of :py:class:`~.Project.Job` can always be used to retrieve the associated *state point*, the *job id*, and the *workspace* directory with the :py:attr:`~.Project.Job.statepoint` attribute, the :py:meth:`~.Project.Job.get_id` method, and the :py:meth:`~.Project.Job.workspace` method:

.. code-block:: python

    >>> print(job.statepoint)
    {'a': 0}
    >>> print(job.get_id())
    9bfd29df07674bc4aa960cf661b5acd2
    >>> print(job.workspace())
    '/home/johndoe/my_project/workspace/9bfd29df07674bc4aa960cf661b5acd2'

The :py:attr:`~.Job.statepoint` attribute provides direct read-write access to the job's state point values, even when these are nested.
For example, given that we have a state point of the form `{'a': 0, 'b': {'c': 1}}`, we can access those values like this:

.. code-block:: python

   >>> job = project.open_job({'a': 0, 'b': {'c': 1}})
   >>> print(job.statepoint.a)
   0
   >>> print(job.statepoint.b.c)
   1

You can also directly modify these values, which will trigger a recalculation of the job id and a renaming of the job's workspace directory.

.. tip::

   The job statepoint attribute can also be accesed through its **alias** :py:attr:`.Job.sp`!

.. _job-document:

The Job Document
----------------

To associate arbitrary metadata with your job without changing the state point, use the job :py:attr:`~.Job.document`!
To associate simple key-value pairs with your job, you can use the job :py:attr:`~.Project.Job.document`.
The document is automatically stored in the job's workspace directory in `JSON`_ format.

.. _`JSON`: https://en.wikipedia.org/wiki/JSON

.. code-block:: python

    >>> job = project.open_job(statepoint)
    >>> job.document['hello'] = 'world'

Just like the job *state point*, individual keys may be accessed as attributes, *e.g.*:

.. code-block:: python

    >>> print(job.document.hello)
    world

.. tip::

     Use the :py:meth:`Job.document.get` method to return ``None`` or another specified default value for missing values. This works exactly like with python's `built-in dictionaries <https://docs.python.org/3/library/stdtypes.html#dict.get>`_.

Use cases for the **job document** include, but are not limited to:

  1) **storage** of *lightweight* data,
  2) keeping track of **runtime information** or to
  3) **label** jobs, e.g. to identify error states.

.. tip::

   The job document can also be accessed through its **alias** :py:attr:`.Job.doc`!

Finding Jobs
------------

You can iterate over all initialized jobs using the :py:meth:`~.Project.find_jobs` method:

.. code-block:: python

    for job in project.find_jobs():
        pass

.. tip::

    Since iterating over all jobs, that means ommitting the ``filter`` argument or setting it to ``None``, is a very common pattern, you can use the following short-hand notation:

    .. code-block:: python

        for job in project:
            pass

Or you can select a subspace by defining a *filter*.
For example, to iterate over all jobs that have a *state point* parameter ``b=0``, execute:

.. code-block:: python

    for job in project.find_jobs({'b': 0}):
        pass

For more information on how to search for specific jobs in Python and on the command line, please see the :ref:`searching` chapter.

.. _modify-statepoint:

Modifying the State Point
-------------------------

It may be necessary to change the state point of one or more jobs after initialization--for example, to add previously not needed state point values.
Modifying a state point entails modifying the job id which means that the state point file needs to be rewritten and the job's workspace directory is renamed, both of which are computationally cheap operations.
The user is nevertheless advised **to take great care when modifying a job's state point** since errors may render the data space **inconsistent**.

There are three main options for modifying a job's state point:

    1. Directly via the job's :py:attr:`~.Project.Job.sp` attribute,
    2. via the job's :py:meth:`~.Project.Job.update_statepoint` method, and
    3. via the job's :py:meth:`~.Project.Job.reset_statepoint` method.

The :py:meth:`~.Project.Job.update_statepoint` method provides safe-guards against accidental overwriting of existing *state point* values, while :py:meth:`~.Project.Job.reset_statepoint` will simply reset the whole *state point* without further questions.
The :py:attr:`~.Project.Job.sp` attribute provides the greatest flexibility, but similar to :py:meth:`~.Project.Job.reset_statepoint` no additional protection.

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
Alternatively, we could also store the result in the :ref:`job document <job-document>`:

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
===============

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

Moving, Copying and Removal
===========================

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


