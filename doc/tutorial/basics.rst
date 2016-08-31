.. _tutorial_minimal:

===============
Getting started
===============

Prerequisites
=============

Installation
------------

This tutorial requires signac, so make sure to install the package before starting.
The easiest way to do so is using conda:

.. code-block:: bash

    $ conda install -c glotzer signac

or pip:

.. code-block:: bash

   $ pip install signac --user

Please refer to the :ref:`installation page <installation>` for detailed instructions on how to install signac.
After successful installation we should be able to open a python shell and import the signac package without error:

.. code-block:: bash

    $ python
    >>> import signac
    >>>

Executing ``signac`` on the command line should prompt something like:

.. code-block:: bash

    $ signac
    usage: signac [-h] [--debug] [--version] [-y]
              {init,project,job,statepoint,index,find,view,config} ...

.. note::

    In some cases the installation routine fails to place the ``signac`` executable on the correct path.
    In this case you may need to adjust your ``$PATH`` configuration manually.
    However, this is not required to complete this tutorial and you can always replace ``$ signac`` with
    ``$ python -m signac``.

Project Setup
-------------

To start we create a new project directory.
You can create the directory anywhere, for example in your home directory.
Initialize your project either on the command line

.. code-block:: bash

    $ mkdir ideal_gas_project
    $ cd ideal_gas_project
    $ signac init IdealGasProject
    Initialized project 'IdealGasProject'.

or within python:

.. code-block:: python

   >>> import signac
   >>> project = signac.init_project('IdealGasProject')
   >>>

This creates a config file ``signac.rc`` within our project root directory with the following content:

.. code-block:: bash

    project=IdealGasProject

Alternatively you can create the config file manually, e.g., with ``$ echo "project=IdealGasProject" > signac.rc``.

The project is the interface to our data space.
We can either interact with it on the command line or use the python interface:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> print(project)
    IdealGasProject

A minimal Example
=================

For this tutorial we want to compute the volume of an ideal gas as a function of its pressure and temperature.

.. math::

    p V = N k_B T

We will set :math:`k_B=1` and execute the complete study in **7 lines** of code:

.. code-block:: python

    0. # minimal.py
    1. import signac
    2. project = signac.get_project()
    3. for p in 0.1, 1.0, 10.0:
    4.     sp = {'p': p, 'T': 10.0, 'N': 10}
    5.     with project.open_job(sp) as job:
    6.         if 'V' not in job.document:
    7.             job.document['V'] = sp['N'] * sp['T'] / sp['p']

1. Import the ``signac`` package.
2. Obtain a handle for the configured project.
3. Iterate over the variable of interest and
4. specify a complete state point.
5. Obtain a ``job`` handle, which associates the state point with our data.
6. Only if the result is not available,
7. compute the result and store it in the *job document*.

We can then examine our results by iterating over the data space:

.. code-block:: python

    >>> for job in project.find_jobs():
    ...     print(job.statepoint()['p'], job.document['V'])
    ...
    0.1 1000.0
    1.0 100.0
    10.0 10.0

This concludes the minimal example.
In the next section we will assume that the ideal gas computation represents a more expensive computation.
We will also take a closer look at the individual components and learn how to operate with files.

.. _tutorial_basics:

The Basics
==========

Data space initialization
-------------------------

In the minimal example we initialized the data space *implicitly*.
Let's see how we can initialize it *explicitly*.
In general, the data space needs to contain all parameters that will affect our data.
For the ideal gas that is a 3-dimensional space spanned by the temperature *T*, the pressure *p* and the system size *N*.

Each state point represents a unique set of parameters that we want to associate with data.
In terms of signac this relationship is represented by a :py:class:`~signac.contrib.job.Job`.

If you ran the minimal example before, you should now remove any previous results with ``$ rm -r workspace``.

Let's define our initialization routine in a script called ``init.py``:

.. code-block:: python

    # init.py
    import signac

    project = signac.get_project()
    for pressure in 0.1, 1.0, 10.0:
        statepoint = {'p': pressure, 'T': 1.0, 'N': 1000}
        job = project.open_job(statepoint)
        job.init()
        print(job, 'initialized')

We can now initialize the workspace with:

.. code-block:: bash

    $ python init.py
    3daa7dc28de43a2ff132a4b48c6abe0e initialized
    9e100da58ccdf6ad7941fce7d14deeb5 initialized
    07dc3f53615713900208803484b87253 initialized

The output shows the job ids associated with each state point.
The *job id* is a unique identifier representing the state point.
Typical computational studies require vastly more parameters than the three we need for the ideal gas computation.
Especially in those cases the *job id* is a much more compact representation of the whole state point.

As we did not explicitly specify the location of our project's *workspace* it defaulted to ``ideal_gas_project/workspace``.
The project's workspace has been populated with directories for each state point:

.. code-block:: bash

   $ ls -1 workspace/
   07dc3f53615713900208803484b87253
   3daa7dc28de43a2ff132a4b48c6abe0e
   9e100da58ccdf6ad7941fce7d14deeb5

We could execute the initialization script multiple times to add more state points, already existing jobs will be ignored.

Computing data
--------------

Now we can finally go ahead and perform our computation.
For this we define two functions inside a ``run.py`` script:

.. code-block:: python

    # run.py

    def calc_volume(N, T, p):
        "Compute the volume of an ideal gas."
        return N * T / p

    def compute_volume(job):
        "Compute the volume of this state point."
        sp = job.statepoint()
        with job:
            V = calc_volume(sp['N'], sp['T'], sp['p'])
            with open('V.txt', 'w') as file:
                file.write(str(V)+'\n')
            print(job, 'computed volume')

The ``calc_volume()`` function returns the volume of an ideal gas with a system size *N*, temperature *T* and pressure *p*.
The ``compute_volume()`` function retrieves the state point from the job argument and stores the result of the ideal gas law calculation in a file called ``V.txt``.
The ``with job:`` clause utilizes the ``job`` handle as a context manager.
It means that all commands below it are executed within the job's workspace directory.
This is good practice, because it means that files are being put into the right location.

We split the computation into two distinct functions to highlight the concept of *operations*.
The ``calc_volume`` function is a pure function with no side-effects, it returns the volume of an ideal gas for a set of input arguments.
In contrast, the ``compute_volume()`` function *modifies* or *operates* on the data space.
Because of this, we call such a function an *operation*.
Any well-defined *operation* should only take one or more arguments of type :py:class:`~signac.contrib.job.Job`.

To execute the ideal gas computation for the whole data space we use signac's capability of iterating over the workspace.
Let's add a few more lines to complete the ``run.py`` script:

.. code-block:: python

    # run.py
    import signac  # <- Add import statement!

    def calc_volume(N, T, p):
        "Compute the volume of an ideal gas."
        return N * T / p

    def compute_volume(job):
        "Compute the volume of this state point."
        sp = job.statepoint()
        with job:
            V = calc_volume(sp['N'], sp['T'], sp['p'])
            with open('V.txt', 'w') as file:
                file.write(str(V)+'\n')
            print(job, 'computed volume')

    project = signac.get_project()
    for job in project.find_jobs():
        compute_volume(job)

We are now ready to execute:

.. code-block:: bash

    $ python run.py
    07dc3f53615713900208803484b87253 computed volume
    3daa7dc28de43a2ff132a4b48c6abe0e computed volume
    9e100da58ccdf6ad7941fce7d14deeb5 computed volume

And we can verify that we actually stored data:

.. code-block:: bash

    $ cat workspace/07dc3f53615713900208803484b87253/V.txt
    100.0

Analyzing data
--------------

Let's examine the results of our computation, by adding an ``examine.py`` script to our project:

.. code-block:: python

    # examine.py
    import signac

    def get_volume(job):
        "Return the computed volume for this job."
        with open(job.fn('V.txt')) as file:
            return float(file.read())

    project = signac.get_project()
    print('p    V')
    for job in project.find_jobs():
        p = job.statepoint()['p']
        V = get_volume(job)
        print('{:04.1f} {}'.format(p, V))

We use the :py:meth:`~signac.contrib.job.Job.fn` function to prepend our filename with the associated workspace path.
Executing this script will print the results to screen:

.. code-block:: bash

   $ python examine.py
   p    V
   00.1 10000.0
   01.0 1000.0
   10.0 100.0

We see that increasing the pressure reduces the volume linearly, exactly what we expect from an ideal gas.
Ordering the output if necessary and/or plotting it is left as an exercise to the reader.

The job document
----------------

So far we have stored the results of our computation in a file.
This is a very viable option, however in this case, as shown in the minimal example, we could also use the *job document*.
The *job document* is a JSON dictionary associated with each job designed to store lightweight data.

To use the job document instead of a file, we need to modify our operation function:

.. code-block:: python

    def compute_volume(job):
        sp = job.statepoint()
        with job:
            V = calc_volume(sp['N'], sp['T'], sp['N'])
            job.document['V'] = V                         # <-- new line
            with open('V.txt', 'w') as file:
                file.write(str(V)+'\n')
            print(job, 'computed volume')

We keep the now redundant writing to the ``V.txt`` file for the sake of being able to demonstrate how to work with files in other parts of the tutorial.

However we can get rid of the ``get_volume()`` function and retrieve the value directly:

.. code-block:: python

    # examine.py
    import signac
    project = signac.get_project()
    print('p    V')
    for job in project.find_jobs():
        p = job.statepoint()['p']
        V = job.document['V']
        print('{:04.1f} {}'.format(p, V))

.. tip::

  If we wanted to make our result display less prone to missing values, we could write ``V = job.document.get('V')`` instead, which will return ``None`` or any other value specified by an optional second argument, in case that the value is missing.

That's it.
We successfully created a well-defined data space for our ideal gas computer experiment.
In the next section we will learn how to search and explore the data space.
