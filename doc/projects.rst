Projects
========

Project setup
-------------

Specifying a project name as identifier within a configuration file initiates a signac project.

.. code-block:: bash

    $ mkdir my_project
    $ cd my_project
    $ echo project=MyProject >> signac.rc

The directory that contains this configuration file is the project's root directory.

Access to project data
-----------------------

You can access your signac :class:`~signac.contrib.project.Project` from within your project's root directory or any subdirectory with the :py:func:`~signac.contrib.get_project` function.

.. code-block:: python

    $ python
    >>> import signac
    >>> project = signac.contrib.get_project()
    >>> print(project)
    MyProject

You can use the project to store data associated with a unique set of parameters.
Parameters are defined by a mapping of key-value pairs stored for example in a :py:class:`dict` object.
Each statepoint is a associated with a unique hash value, called *job id*.
Get an instance of :py:class:`~signac.contrib.job.Job`, which is a handle on your job's data space with the :py:meth:`~signac.contrib.project.Project.open_job` method.

.. code-block:: python

    # define a statepoint
    >>> statepoint = {'a': 0}
    # get the associated job
    >>> job = project.open_job(statepoint)
    >>> job.get_id()
    '9bfd29df07674bc4aa960cf661b5acd2'

You can use the job id to organize your data.


The workspace
-------------

You can specify a workspace directory that signac uses to store job data in.

.. code-block:: bash

    $ echo workspace_dir=/path/to/my/workspace/for/my_project >> signac.rc

.. note::

    Although it is not required, the workspace can of course be within the project directory.

This gives you access to a unique path for each job within your workspace directory.

.. code-block:: python

    >>> job.workspace()
    '/path/to/my/workspace/for/my_project/9bfd29df07674bc4aa960cf661b5acd2'

A convenient way to switch between workspaces is to use the :py:class:`~signac.contrib.job.Job` as `context manager`_:
This will switch to the job's workspace after entering the context and switches back to the original working directory after exiting.

.. _`context manager`: http://effbot.org/zone/python-with-statement.htm

.. code-block:: python

    >>> with project.open_job(statepoint) as job:
    >>>   with open('myfile.txt', 'w') as file:
    >>>     file.write('hello world')
    >>>   print(os.listdir(job.workspace()))
    ['myfile.txt']
    >>>

The job document
----------------

To associate simple key-value pairs with your job, you can use the job :py:attr:`~signac.contrib.job.Job.document`.
The document is automatically stored in the job's workspace directory in JSON format.

.. code-block:: python

    >>> job = project.open_job(statepoint)
    >>> job.document['hello'] = 'world'

Operate on the workspace
------------------------

Using a workspace makes it easy to keep track of your parameter space.
Use :py:meth:`~signac.contrib.project.Project.get_statepoints` to retrieve a list of all statepoints for jobs with data in your workspace.

.. code-block:: python

    >>> statepoints = [{'a': i} for i in range(5)]
    >>> for statepoint in statepoints:
    ...   with project.open_job(statepoint) as job:
              # Entering the job context once will trigger
              # the creation of the workspace directory.
              pass
    ...
    >>> project.find_statepoints()
    [{'a': 3}, {'a': 4}, {'a': 1}, {'a': 0}, {'a': 2}]
    >>>


If you want to operate on all or a select number of jobs, use :py:meth:`~signac.contrib.project.Project.find_jobs` which will yield all or a filtered set of :py:class:`~signac.contrib.job.Job` instances.

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
