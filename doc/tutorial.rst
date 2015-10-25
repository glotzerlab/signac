========
Tutorial
========

Setting up a signac project
===========================

Specifying a project name as identifier within a configuration file initiates a signac project.

.. code:: bash

    $ mkdir my_project
    $ cd my_project
    $ echo project=MyProject >> signac.rc
   
The directory that contains this configuration file is the project's root directory.
You can specify a workspace directory that signac uses to store job data in.

.. code:: bash

    $ echo workspace_dir=/path/to/my/workspace/for/my_project >> signac.rc

You can access your signac :class:`~signac.contrib.project.Project` from within your project's root directory or any subdirectory with

.. code:: python

    $ python
    >>> import signac
    >>> project = signac.contrib.get_project()
    >>> print(project)
    MyProject

You can use the project to store data associated to a unique set of parameters.
Parameters are defined by a mapping of key-value pairs stored for example in a :py:class:`dict` object.

.. code:: python
  
    # define a statepoint
    >> statepoint = {'a': 0}
    # get the associated job
    >> job = project.open_job(statepoint)

Each statepoint is a associated with a unique hash value that defines the job's unique identifier.

.. code:: python

    >> job.get_id()
    '9bfd29df07674bc4aa960cf661b5acd2'

You can use the job id to organize your data.
If you configured a workspace directory for your project you can obtain a unique path for each job.

.. code:: python

    >> job.workspace()
    '/path/to/my/workspace/for/my_project/9bfd29df07674bc4aa960cf661b5acd2'
