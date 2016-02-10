Projects
========

Project setup
-------------

Specifying a project name as identifier within a configuration file initiates a signac project.

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject
    # or
    $ echo project=MyProject >> signac.rc

The directory that contains this configuration file is the project's root directory.

Verify the configuration with:

.. code-block:: bash

    $ signac project
    MyProject

.. note::

    Some of the functions introduced here on the python level have command line equivalents, to make it easier to integrate **signac** with bash scripts.
    Check out ``$ signac --help`` for more information.


Access to project data
-----------------------

You can access your signac :class:`~signac.contrib.Project` from within your project's root directory or any subdirectory from the command line:

.. code-block:: shell

    $ signac project
    MyProject

Or with the :py:func:`~signac.get_project` function:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> print(project)
    MyProject


You can use the project to store data associated with a unique set of parameters, called a *state point*.
Parameters are defined by a mapping of key-value pairs stored for example in a :py:class:`dict` object or in JSON format.
Each state point is a associated with a unique hash value, called *job id*.
Get an instance of :py:class:`~signac.contrib.job.Job`, which is a handle on your job's data space with the :py:meth:`~signac.contrib.project.Project.open_job` method.

.. code-block:: python

    # Define a state point:
    >>> statepoint = {'a': 0}
    # Get the associated job:
    >>> job = project.open_job(statepoint)
    >>> job.get_id()
    '9bfd29df07674bc4aa960cf661b5acd2'
    
Equivalent from the command line:

.. code-block:: shell

    $ signac job '{"a": 0}'
    9bfd29df07674bc4aa960cf661b5acd2
    # Pipe large statepoint definitions:
    $ cat mystatepoint.json > signac job
    ab343j...

The *job id* is a unique identifier or address for all project data.

The workspace
-------------

The signac project related data is stored in the **workspace**, which by default is a directory called ``workspace``, located in your project's root directory.
You can configure a different workspace directory with the ``workspace_dir`` attribute, either relative to your project's root directory or as absolute path.

This gives you access to a unique path for each job within your workspace directory.

.. code-block:: python

    >>> job.workspace()
    '/path/to/my/workspace/for/my_project/9bfd29df07674bc4aa960cf661b5acd2'

A convenient way to switch between workspaces is to use the :py:class:`~signac.contrib.job.Job` as `context manager`_:
This will switch to the job's workspace after entering the context and switches back to the original working directory after exiting.

.. _`context manager`: http://effbot.org/zone/python-with-statement.htm

.. code-block:: python

    >>> with project.open_job(statepoint) as job:
    ...     with open('myfile.txt', 'w') as file:
    ...         file.write('hello world')
    ...     print(os.listdir(job.workspace()))
    ...
    ['myfile.txt']
    >>>

Once a job is initialized in the workspace, or the state point was written with :py:meth:`~signac.contrib.Project.write_statepoints` it is possible to **open a job by job id**:

.. code-block:: python

    >>> with project.open_job(id='9bfd29df07674bc4aa960cf661b5acd2') as job:
    ...     print(job.statepoint())
    ...
    {'a': 0}

Operate on the workspace
------------------------

Using a workspace makes it easy to keep track of your parameter space.
Use :py:meth:`~signac.contrib.Project.find_statepoints` to retrieve a list of all state points for jobs with data in your workspace.

.. code-block:: python

    >>> statepoints = [{'a': i} for i in range(5)]
    >>> for statepoint in statepoints:
    ...   with project.open_job(statepoint) as job:
    ...       # Entering the job context once will trigger
    ...       # the creation of the workspace directory.
    ...       pass
    ...
    >>> project.find_statepoints()
    [{'a': 3}, {'a': 4}, {'a': 1}, {'a': 0}, {'a': 2}]
    >>>


If you want to operate on all or a select number of jobs, use :py:meth:`~signac.contrib.Project.find_jobs` which will yield all or a filtered set of :py:class:`~signac.contrib.job.Job` instances.

.. code-block:: python

    >>> for job in project.find_jobs():
    ...     print(job, job.statepoint())
    ...
    14fb5d016557165019abaac200785048 {'a': 3}
    2af7905ebe91ada597a8d4bb91a1c0fc {'a': 4}
    42b7b4f2921788ea14dac5566e6f06d0 {'a': 1}
    9bfd29df07674bc4aa960cf661b5acd2 {'a': 0}
    9f8a8e5ba8c70c774d410a9107e2a32b {'a': 2}
    >>>
    >>> for job in project.find_jobs({'a': 0}):
    ...     print(job, job.statepoint())
    ...
    9bfd29df07674bc4aa960cf661b5acd2 {'a': 0}
    >>>

The job document
----------------

To associate simple key-value pairs with your job, you can use the job :py:attr:`~signac.contrib.job.Job.document`.
The document is automatically stored in the job's workspace directory in JSON format.

.. code-block:: python

    >>> job = project.open_job(statepoint)
    >>> job.document['hello'] = 'world'

Uses cases for the **job document** include, but are not limited to:

  1) **storage** of *lightweight* data,
  2) keeping track of **runtime information** or to
  3) **label** jobs, e.g. to identify error states.

You can use job documents in combination with a database to execute complex query operations.
In the following example, all job documents contain a field called `user_status`, which contains a list of labels that help to identify the job status.

.. code-block:: python

    >>> for job in project.find_jobs():
    ...     ## identify the labels
    ...     print(job.document['user_status'])
    ...
    ['stage2', 'walltimelimitreached']
    ['stage3', 'done']
    >>> # etc

Using the :py:meth:`~signac.contrib.Project.find_job_documents` method, we can export all or a subset of the **job documents** into a database to execute more complex query operations.

.. code-block:: python

    >>> # We want to export the job documents to a MongoDB document collection.
    >>> job_docs_collection = signac.get_database('MyProject').job_docs
    >>> # Get a list of all or a subset of the job documents
    >>> job_docs = list(project.find_job_documents())
    >>> # Export to the collection
    >>> job_docs_collection.insert_many(job_docs)

To find all jobs labeled with 'stage2' that ran out of walltime we could execute the following query:

.. code-block:: python

    >>> jobs_stage2 = job_docs_collection.find({'user_status': ['stage2', 'walltimelimitreached']})

Create workspace views
----------------------

Job ids are extremely useful to manage vast parameter spaces,
however at the same time make it impossible to identify state points by
browsing through the file system.
In this case you can create a **view** on all or parts of the data
with human-readable state points using the
:py:meth:`~signac.contrib.Project.create_view` method.

A view is a directory hierarchy consisting of **symbolic links**
to the job workspace directories.
This means no data is copied but you can conveniently browse through
the job data space.

Let's assume the parameter space is

    * a=0, b=0
    * a=1, b=0
    * a=2, b=0
    * ...,

where *b* does not vary over all state points.

Calling :py:meth:`~signac.contrib.Project.create_view()` will generate
the following *symbolic links* within the specified  view directory:

.. code:: bash

    view/a/0 -> /path/to/workspace/7f9fb369851609ce9cb91404549393f3
    view/a/1 -> /path/to/workspace/017d53deb17a290d8b0d2ae02fa8bd9d
    ...

.. note::

    As *b* does not vary over the whole parameter space it is not part
    of the view url.
    This maximizes the compactness of each view url.

.. hint::

    Using a **filter** argument, you can create a view for a **subset**
    of the data, e.g.:

    .. code:: python

        >>> project.create_view({'debug': True})

    will create links only for jobs where *debug* equals *True*.
