.. _tutorial:

========
Tutorial
========

This tutorial demonstrates how to implement a basic computational workflow with signac.
We will conduct a simple computational investigation using the ideal gas law.
While using signac for this study is absolute overkill it will help us to learn the basics for more involved future studies.

The tutorial assumes a basic proficiency in python and will take about 20 to 30 minutes to complete.

Basics
======

Prerequisites
-------------

This tutorial requires signac, so make sure to install the package before starting.
The signac framework assists us in conducting a computational investigation by managing the data space for us.
This means that we do not need to worry about how to organize our data on disk and how to keep track of meta data.

For the first part of the tutorial we only need signac, for the other parts we also need signac-flow.
The easiest way to install both packages is using conda:

.. code-block:: bash

    $ conda install signac

Alternative installation methods are described :ref:`here <installation>`.

Project Setup
-------------

To start we create a new project directory.
You can create the directory anywhere, for example in your home directory.

.. code-block:: bash

    $ mkdir ideal_gas_project
    $ cd ideal_gas_project
    $ signac init IdealGasProject
    Initialized project 'IdealGasProject'.

This creates a config file ``signac.rc`` within our project root directory.
Executing ``signac project`` on the command line anywhere within our project directory returns the name of the configured project:

.. code-block:: bash

    $ signac project
    IdealGasProject

The project is the interface to our data space.
We can either interact with it on the command line like we just did or use the python interface:

.. code-block:: python

    >>> import signac
    >>>
    >>> project = signac.get_project()
    >>> print(project)
    IdealGasProject

Data space initialization
-------------------------

For this tutorial we want to compute the volume of an ideal gas as a function of its pressure and temperature.
Before we can compute anything we need to initialize our parameter space.
The parameter space needs to span all parameters that will affect our data.
For the ideal gas that is a 3-dimensional space spanned by the temperature *T*, the pressure *p* and the system size *N*.

To initialize the parameter space we will iterate over the variable and create a statepoint.

.. code-block:: python

    for pressure in 0.1, 1.0, 10.0:
        statepoint = {'p': pressure, 'T': 1.0, 'N': 1000}

This statepoint represents a unique set of parameters that we want to associate with data.
In terms of signac this relationship is represented by a :py:class:`~signac.contrib.job.Job`.
Let's initialize all jobs for our study in a script called ``init.py``:

.. code-block:: python

    # init.py
    import signac

    project = signac.get_project()
    for pressure in 0.1, 1.0, 10.0:
        statepoint = {'p': pressure, 'T': 1.0, 'N': 1000}
        job = project.open_job(statepoint)
        job.init()
        print('initialized', job)

We can now initialize the workspace with:

.. code-block:: bash

    $ python init.py
    initialized 3daa7dc28de43a2ff132a4b48c6abe0e
    initialized 9e100da58ccdf6ad7941fce7d14deeb5
    initialized 07dc3f53615713900208803484b87253

The output shows the job ids associated with each statepoint.
The *job id* is a unique identifier representing the statepoint.
Typical computational studies require vastly more parameters than the three we need for the ideal gas computation.
Especially in those cases the *job id* is a much more compact representation of the whole statepoint.

As we did not explicitely specify the location of our project's *workspace* it defaulted to 'ideal_gas_project/workspace'.
The project's workspace has been populated with directories for each statepoint:

.. code-block:: bash

   $ ls workspace/
   07dc3f53615713900208803484b87253        3daa7dc28de43a2ff132a4b48c6abe0e        9e100da58ccdf6ad7941fce7d14deeb5

We could execute the initialization script multiple times to add more statepoints, already existing jobs will be ignored.

Computing results
-----------------

Now we can finally go ahead and perform our "simulation".
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
The ``compute_volume()`` function retrieves the statepoint from the job argument and stores the result of the ideal gas law calculation in a file called ``V.txt``.
The ``with job:`` clause utilizes the ``job`` handle as a context manager.
It means that all commands below it are executed within the job's workspace directory.
This is good practice, because it means that files are being put into the right location.

We split this computation into two distinct function to highlight the concept of *operations*.
The ``calc_volume`` function is a pure function with no side-effects, it returns the volume of an ideal gas for a set of input arguments.
In contrast, the ``compute_volume()`` function *modifies* or *operates* on the data space.
Because of this, we call such a function an *operation*.
Any well-defined *operation* should only take one or more arguments of type :py:class:`~signac.contrib.job.Job`.

To execute our "ideal gas simulator" for the whole data space we use signac's capability of iterating over the workspace.
Let's add a few more lines to complete the ``run.py`` script:

.. code-block:: python

    # run.py
    import signac

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

Analyzing results
-----------------

Let's examine the results of our computation, by adding an ``examine.py`` script to our project:

.. code-block:: python

    # examine.py
    import os
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

We use the ``job.fn()`` function to prepend our filename with the associated workspace path.
Executing this script will print the results to screen:

.. code-block:: bash

   $ python examine.py
   p    V
   00.1 10000.0
   01.0 1000.0
   10.0 100.0

We see that increasing the pressure reduces the volume linearly, exactly what we expect from an ideal gas.
Ordering the output if necessary and/or plotting it is left as an exercise to the reader.

Streamlining the workflow
=========================

Classification
--------------

Let's imagine we are still not convinced of the relationship that we just "discovered" and want to add a few more statepoints.
We can do so by modifying the ``init.py`` script:

.. code-block:: python

    # init.py
    import signac
    import numpy as np  # <-- importing numpy

    project = signac.get_project()
    for pressure in np.linspace(0.1, 10.0, 10):  # <-- using linspace()
        statepoint = {'p': pressure, 'T': 10.0, 'N': 10}
        job = project.open_job(statepoint)
        job.init()
        print(job, 'initialized')

Running ``$ python init.py`` again will initialize a few more statepoint, but now we have a problem.
If we were not using the ideal gas law, but a more complicated simulation we would want to skip all statepoints that have already been computed.

One way is to add a simple check to our ``run.py`` script:

.. code-block:: python

      for job in project.find_jobs():
          if job.isfile('V.txt'):
              continue
          else:
              compute_volume(job)

It would be even better if we could get an overview of which statepoints have been computed and which not.
We call this a project's *status*.

For this purpose we classify each *job* based on certain conditions.
We label our *jobs* based on certain conditions with a ``classify()`` generator function:

.. code-block:: python

      def classify(job):
          yield 'init'
          if job.isfile('V.txt'):
              yield 'volume-computed'

Our classifier will always yield the ``init`` label, but the ``volume-computed`` label is only yielded if the result file exists.
We can then embed this function in a ``project.py`` script to view our project's status:

.. code-block:: python

    # project.py
    import signac

    def classify(job):
        yield 'init'
        if job.isfile('V.txt'):
            yield 'volume-computed'

    if __name__ == '__main__':
        project = signac.get_project()
        print(project)

        for job in project.find_jobs():
            labels = ','.join(classify(job))
            p = '{:04.1f}'.format(job.statepoint()['p'])
            print(job, p, labels)

Executing this script should show us that the statepoints that we initialized earlier have been evaluated, but the new ones have not:

.. code-block:: bash

    $ python project.py
    07dc3f53615713900208803484b87253 10.0 init,volume-computed
    14ba699529683f7132c863c51facc79c 04.5 init
    184f2b7e8eadfcbc9f7c4b6638db3c43 07.8 init
    30e9e87d9ae2931df88787e105506cb2 05.6 init
    3daa7dc28de43a2ff132a4b48c6abe0e 00.1 init,volume-computed
    474778977e728a74b4ebc2e14221bef6 03.4 init
    6869bef5f259337db37b11dec88f6fab 06.7 init
    9100165ad7753e91804f1eb875ea0b69 01.2 init
    957349e42149cea3b0362226535a3973 08.9 init
    9e100da58ccdf6ad7941fce7d14deeb5 01.0 init,volume-computed
    b0dd91c4755b81b47becf83e6fb22413 02.3 init

We can use the classification to control execution in ``run.py``:

.. code-block:: python

    # run.py
    import signac
    from project import classify

    # ...

    for job in project.find_jobs():
        if 'volume-computed' not in classify(job):
            compute_volume(job)

This ensures that we only execute ``compute_volume()`` for the 8 new statepoints:

.. code-block:: bash

    $ python run.py
    14ba699529683f7132c863c51facc79c computed volume
    184f2b7e8eadfcbc9f7c4b6638db3c43 computed volume
    30e9e87d9ae2931df88787e105506cb2 computed volume
    474778977e728a74b4ebc2e14221bef6 computed volume
    6869bef5f259337db37b11dec88f6fab computed volume
    9100165ad7753e91804f1eb875ea0b69 computed volume
    957349e42149cea3b0362226535a3973 computed volume
    b0dd91c4755b81b47becf83e6fb22413 computed volume


Determining the next operation
------------------------------

In an effort to make our workflow high-performance cluster compatible we split the definition of operations and the execution into two different modules.
We move the ``calc_volume()`` and ``compute_volume()`` functions into an ``operations.py`` module:

.. code-block:: python

    # operations.py
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

We then determine the next operation explicitly by adding a ``next_operation()`` function in the ``project.py`` module:

.. code-block:: python
    
    # project.py

    # ...

    def next_operation(job):
        if 'volume-computed' not in classify(job):
            return 'compute_volume'

And use it for execution in the ``run.py`` script:

.. code-block:: python

    # run.py
    import signac
    import operations
    from project import next_operation

    project = signac.get_project()
    for job in project.find_jobs():
        next_op = next_operation(job)
        if next_op is not None:
            func = getattr(operations, next_op)
            func(job)

The ``func`` variable contains a reference to a function defined in the ``operations.py`` module with the same name as our *next operation*.
In other words, we can execute any function defined in the ``operations.py`` module by returning its name in the ``next_operation()`` function.

.. tip::

    Specify the output verbosity with the :py:mod:`logging` module, for example by adding the following lines to the ``run.py`` script:

    .. code-block:: python
        
        import logging
        logging.basicConfig(level=logging.INFO)

Summary
-------

This completes the workflow that we wanted to implement.
We created the following layout:

  * ``init.py``: Initialize the project's data space.
  * ``project.py``: Implements classification and project workflow logic.
  * ``operations.py``: Implements how we operate on the projects' data space.
  * ``run.py``: Execution of said operations.
  * ``examine.py``: Aggregates and prints results to screen.

What's left
===========

The job document
----------------

So far we have stored the results of our computation in a file.
This is a very viable option, however in this case we could also use the *job document*.
The *job document* is a JSON dictionary associated with each job designed to store lightweight data.

To use the job document instead of a file, we need to modify our operation function:

.. code-block:: python

    def compute_volume(job):
        sp = job.statepoint()
        with job:
            V = calc_volume(sp['N'], sp['T'], sp['N'])
            job.document['V'] = V
            print(job, 'computed volume')
          
Technically using the ``with job:`` clause is not necessary in this case, but we'll keep it in there for good measure.
Now we need to modify our classification function:

.. code-block:: python

    def classify(job):
        yield 'init'
        if 'V' in job.document:
            yield 'volume-computed'

Finally, we get rid of the ``get_volume()`` function and retrieve the value directly:

.. code-block:: python

    # examine.py
    import signac
    print('p    V')
    for job in project.find_jobs():
        p = job.statepoint()['p']
        V = job.document['V']
        print('{:04.1f} {}'.format(p, V))

If we wanted to make our result display less prone to missing values, we could use ``V = job.document.get('V')`` instead, which will return ``None`` or any other value specified by an optional second argument, in case that the value is missing.

Views
-----

Sometimes we want to examine our data on the file system directly.
However the file paths within the workspace are obfuscated by the *job id*.
The solution is to use *views*, which are human-readable, but maximal compact hierarchical links to our data space.

To create a view we simply execute:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> project.create_view()

This creates a directory called ``view`` which contains the view links:

.. code-block:: bash

    ls view/p
    0.1  1.0  10.0  1.2  2.3  3.4  4.5  5.6  6.7  7.8  8.9

This allows us to examine the data with human-readable path names:

.. code-block:: bash

    cat view/p/10.0/V.txt
    100.0

.. note:: 
      
    The actual file paths will slightly differ because of floating point precision.

Indexing
--------

*Coming soon.*
