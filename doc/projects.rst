Projects
========

Setup a project
---------------

Specifying a project name as identifier within a configuration file initiates a signac project.

.. code:: bash

    $ mkdir my_project
    $ cd my_project
    $ echo project=MyProject >> signac.rc

The directory that contains this configuration file is the project's root directory.
You can specify a workspace directory that signac uses to store job data in.

.. code:: bash

    $ echo workspace_dir=/path/to/my/workspace/for/my_project >> signac.rc

Access the project data
-----------------------

You can access your signac :class:`~signac.contrib.project.Project` from within your project's root directory or any subdirectory with the :py:func:`~signac.contrib.get_project` function.

.. code:: python

    $ python
    >>> import signac
    >>> project = signac.contrib.get_project()
    >>> print(project)
    MyProject

You can use the project to store data associated with a unique set of parameters.
Parameters are defined by a mapping of key-value pairs stored for example in a :py:class:`dict` object.
Each statepoint is a associated with a unique hash value, called *job id*.
Get an instance of :py:class:`~signac.contrib.job.Job`, which is a handle on your job's data space with the :py:meth:`~signac.contrib.project.Project.open_job` method.

.. code:: python

    # define a statepoint
    >>> statepoint = {'a': 0}
    # get the associated job
    >>> job = project.open_job(statepoint)
    >>> job.get_id()
    '9bfd29df07674bc4aa960cf661b5acd2'

You can use the job id to organize your data.
If you configured a workspace directory for your project you can obtain a unique path for each job.

.. code:: python

    >>> job.workspace()
    '/path/to/my/workspace/for/my_project/9bfd29df07674bc4aa960cf661b5acd2'
