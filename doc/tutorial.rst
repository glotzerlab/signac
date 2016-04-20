.. _tutorial:

========
Tutorial
========

.. sidebar:: Requirementes

    The tutorial assumes a basic proficiency in python and will take about 20 to 30 minutes to complete.

This tutorial demonstrates how to implement a basic computational workflow with signac.
The signac framework assists us in conducting a computational investigation by managing the data space for us.
This means that we do not need to worry about how to organize our data on disk and how to keep track of meta data.

We will conduct a simple computational investigation using the ideal gas law.

Prerequisites
=============

Installation
------------

This tutorial requires signac, so make sure to install the package before starting.
The easiest way to do so is using conda:

.. code-block:: bash

    $ conda install signac

Alternative installation methods are described :ref:`here <installation>`.
After successful installation we should be able to open a python shell and import the signac package without error:

.. code-block:: bash

    $ python
    >>> import signac
    >>>

Executing ``signac`` on the command line should prompt something like:

.. code-block:: bash

    $ signac
    usage: signac [-h] [--debug] [--version] {project,job,init} ...

.. note::

    In some cases the installation routine fails to place the ``signac`` executable on the correct path.
    In this case you may need to adjust your ``PATH`` configuration manually.
    Howver, this is not required to complete this tutorial.

Project Setup
-------------

To start we create a new project directory.
You can create the directory anywhere, for example in your home directory.

.. code-block:: bash

    $ mkdir ideal_gas_project
    $ cd ideal_gas_project
    $ signac init IdealGasProject
    Initialized project 'IdealGasProject'.

This creates a config file ``signac.rc`` within our project root directory with the following content:

.. code-block:: bash

    project=IdealGasProject

Alternatively you can create the config file manually with ``$ echo "project=IdealGasProject" > signac.rc``.

The project is the interface to our data space.
We can either interact with it on the command line or use the python interface:

.. code-block:: python

    >>> import signac
    >>> project = signac.get_project()
    >>> print(project)
    IdealGasProject

The minimal Example
===================

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
    0.1 10000.0
    1.0 1000.0
    10.0 100.0

This concludes the minimal example.
In the next section we will assume that the ideal gas computation represents a more expensive computation.
We will also take a closer look at the individual components and learn how to operate with files.

The Basics
==========

Data space initialization
-------------------------

In the minimal example we initialized the data space *implicitely*.
Let's see how we can initialize it *explicitely*.
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

As we did not explicitely specify the location of our project's *workspace* it defaulted to ``ideal_gas_project/workspace``.
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

Analyzing data
--------------

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
In the next section we will complete our workflow to make it more flexible.

A complete Workflow
===================

Classification
--------------

Let's imagine we are still not convinced of the relationship that we just "discovered" and want to add a few more state points.
We can do so by modifying the ``init.py`` script:

.. code-block:: python

    # init.py
    import signac
    import numpy as np                            # <-- importing numpy

    project = signac.get_project()
    for pressure in np.linspace(0.1, 10.0, 10):   # <-- using linspace()
        statepoint = {'p': pressure, 'T': 10.0, 'N': 10}
        job = project.open_job(statepoint)
        job.init()
        print(job, 'initialized')

Running ``$ python init.py`` again will initialize a few more state points, but now we have a problem.
If we were not using the ideal gas law, but a more expensive simulation we would want to skip all state points that have already been computed.

One way is to add a simple check to our ``run.py`` script:


.. code-block:: python

      for job in project.find_jobs():
          if 'V' not in job.document:
              compute_volume(job)

.. tip::

      Use :py:meth:`~signac.contrib.job.Job.isfile` to implement the same check for the file solution:

      .. code-block:: python

          for job in project.find_jobs():
              if not job.isfile('V.txt'):
                  compute_volume(job)

It would be even better if we could get an overview of which state points have been computed and which not.
We call this a project's *status*.

For this purpose we classify each *job* by attaching labels.
We label our *jobs* based on certain conditions with a ``classify()`` generator function:

.. code-block:: python

      def classify(job):
          yield 'init'
          if 'V' in job.document:
              yield 'volume-computed'

Our classifier will always yield the ``init`` label, but the ``volume-computed`` label is only yielded if the result has already been computed.
We can then embed this function in a ``project.py`` script to view our project's status:

.. code-block:: python

    # project.py
    import signac

    def classify(job):
        yield 'init'
        if 'V' in job.document:
            yield 'volume-computed'

    if __name__ == '__main__':
        project = signac.get_project()
        print(project)

        for job in project.find_jobs():
            labels = ','.join(classify(job))
            p = '{:04.1f}'.format(job.statepoint()['p'])
            print(job, p, labels)

Executing this script should show us that the state points that we initialized earlier have been evaluated, but the new ones have not:

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

We can use the classification to control the execution in ``run.py``:

.. code-block:: python

    # run.py
    import signac
    from project import classify

    # ...

    for job in project.find_jobs():
        if 'volume-computed' not in classify(job):
            compute_volume(job)

This ensures that we only execute ``compute_volume()`` for the 8 new state points:

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
            job.document['V'] = V
            print(job, 'computed volume')

We then determine the next operation explicitly by adding a ``next_operation()`` function in the ``project.py`` module:

.. code-block:: python

    # project.py

    # ...

    def next_operation(job):
        if 'volume-computed' not in classify(job):
            return 'compute_volume'

And use its result to control the execution in the ``run.py`` script:

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


.. tip::

    **Don't hesitate to implement lightweight operations directly!**

    The minimal example implements almost **the complete workflow in 7 lines** of code.


Views and Indexing
==================

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

Sometimes it is advantageous to implement your own view routine.
This is an example for a flat linked view:

.. code-block:: python

    # create_flat_view.py
    import os
    import signac

    project = signac.get_project()
    variables = project.find_variable_parameters()[0]
    for job in project.find_jobs():
        name = '_'.join('{}_{}'.format(p, job.statepoint()[p])
                        for p in variables)
        os.symlink(job.fn('V.txt'), name + '_V.txt')

The :py:meth:`~signac.contrib.project.Project.find_variable_parameters` method returns a hierarchical list of all varying parameters.
In our case this is only the pressure *p*.

Executing this, will create multiple symbolic links pointing to the source files with a parameter-based, human-readable name:

.. code-block:: bash

    $ python create_flat_view.py
    $ ls -1 *.txt
    p_0.1_V.txt
    p_10.0_V.txt
    # ...

Indexing
--------

A index is a complete record of the data and its associated metadata within our project's data space.
To create an index, we need to crawl through the project's data space.
To do so, we can either specialize a :py:class:`~signac.contrib.crawler.SignacProjectCrawler` or call the :py:meth:`~signac.Project.index` method.
Let's implement a ``create_index.py`` script:

.. code-block:: python

    # create_index.py
    import signac

    project = signac.get_project()
    for doc in project.index():
        print(doc)

If we used the *job document* for data storage this will immediately generate an index of our data:

.. code-block:: bash

    $ python create_index.py
    474778977e728a74b4ebc2e14221bef6 {'signac_id': '474778977e728a74b4ebc2e14221bef6', 'format': None, 'V': 294.1176470588235, 'statepoint': {'T': 1.0, 'N': 1000, 'p': 3.4000000000000004}, '_id': '474778977e728a74b4ebc2e14221bef6'}
    184f2b7e8eadfcbc9f7c4b6638db3c43 {'signac_id': '184f2b7e8eadfcbc9f7c4b6638db3c43', 'format': None, 'V': 128.2051282051282, 'statepoint': {'T': 1.0, 'N': 1000, 'p': 7.800000000000001}, '_id': '184f2b7e8eadfcbc9f7c4b6638db3c43'}
    3daa7dc28de43a2ff132a4b48c6abe0e {'signac_id': '3daa7dc28de43a2ff132a4b48c6abe0e', 'format': None, 'V': 10000.0, 'statepoint': {'T': 1.0, 'N': 1000, 'p': 0.1}, '_id': '3daa7dc28de43a2ff132a4b48c6abe0e'}
    # ...

If we used text files to store data we need to additionally specify the format of those file to make them *indexable*.
In general, any python class may be a format definition, however optimally a format class provides a file-like interface.
An example for such a format class is the :py:class:`~signac.contrib.formats.TextFile` class.
We will specify that in addition to the *job documents* all files named ``V.txt`` within our data space are to be indexed as *TextFiles*:

.. code-block:: python

    # create_index.py
    import signac
    from signac.contrib.formats import TextFile

    project = signac.get_project()
    for doc in project.index({'.*/V\.txt': TextFile}):
        print(doc)

The regular expression ``.*/V\.txt`` specifies that all files ending in ``V.txt`` are to be indexed, that would include sub-directories!

Using a master crawler
----------------------

A master crawler uses other other crawlers to compile a combined master index of one or more data spaces.
This allows you to expose your project data to you and everyone else who has access to the index.

To expose the project to a :py:class:`~signac.contrib.crawler.MasterCrawler` we need to create an :ref:`access module <signac-access>`.
For signac projects this is simplified by using the :py:meth:`~signac.contrib.project.Project.create_access_module` method.
Let's create the access module by adding the following commands to the ``create_index.py`` script:

.. code-block:: python

    # create_index.py
    # ...
    try:
        project.create_access_module({'.*/V\.txt': TextFile})
    except OSError:
        print("Access module already exists!")

This will create a ``signac_access.py`` module in the project's root directory, which will look like this:

.. code-block:: bash

    #!/usr/bin/env python
    # -*- coding: utf-8 -*-
    import os

    from signac.contrib.crawler import SignacProjectCrawler
    from signac.contrib.formats import TextFile
    from signac.contrib.crawler import MasterCrawler


    class IdealGasProjectCrawler(SignacProjectCrawler):
        pass
    IdealGasProjectCrawler.define('.*/V\.txt', TextFile)


    def get_crawlers(root):
        return {'main': IdealGasProjectCrawler(os.path.join(root, 'workspace'))}


    if __name__ == '__main__':
        master_crawler = MasterCrawler('.')
        for doc in master_crawler.crawl(depth=1):
            print(doc)

The ``signac_access.py`` module defines a specific crawler for this project, which can be further specialized.

A master crawler will search for modules like this, imports them and then executes call crawlers defined in the ``get_crawlers()`` function.
By modifying the access module, you can control exactly what data is exposed to a master crawler.

.. note::

    The expression ``if __name__ == '__main__':`` is only True if the script is directly executed and not imported from another script.
    This means the commands below it have no relevance with regards to the script's function as access module.
    The commands are there to allow immediate testing.

Fetch data via index
--------------------

Data, which was indexed with a :py:class:`~signac.contrib.crawler.MasterCrawler` can be seamlessly fetched using the signac :py:func:`~signac.fetch` and :py:func:`~signac.fetch_one` functions.
Let's test this!

First we create a script to compile a master index in JSON format:

.. code-block:: python

    # create_master_index.py
    import json
    from signac.contrib.crawler import MasterCrawler

    master_crawler = MasterCrawler('.')
    for doc in master_crawler.crawl(depth=1)
        print(json.dumps(doc))

The master crawler is initialized for the current working directory and the index documents are printed to screen in JSON format.

We then store the index in a file:

.. code-block:: bash

    $ python create_master_index.py > index.txt

Next, we implememt a ``fetch.py`` script:

.. code-block:: python

    # fetch.py
    import json
    import signac

    with open('index.txt') as file:
        for line in file:
            doc = json.loads(line)
            file = signac.fetch_one(doc)
            V = float(file.read())
            print(doc['statepoint'], V)

This scripts reads the index documents from the index file.
The index document is stored in the ``doc`` variable and contains the link to the indexed file.
We pass the ``doc`` variable to the :py:func:`~signac.fetch_one` function to open the file and then print its content to screen.

.. code-block:: bash

    $ python fetch.py
    {'p': 10.0, 'N': 1000, 'T': 1.0} 100.0
    {'p': 4.5, 'N': 1000, 'T': 1.0} 222.22222222222223
    {'p': 7.800000000000001, 'N': 1000, 'T': 1.0} 128.2051282051282
    # ...

Database Integration
--------------------

Instead of storing the index in a plain-text file we could export it to any tool of our choice.
For convenience, signac provides export routines for MongoDB database collections.

If we :ref:`configured <configuration>` a MongoDB database we could export the index to a database collection:

.. code-block:: python

    # create_index.py
    import signac

    project = signac.get_project()
    db = signac.get_database('mydb')
    signac.contrib.export_pymongo(project.index(), db.index)

    # Or using a master crawler:
    master_crawler = signac.contrib.crawler.MasterCrawler('.')
    signac.contrib.export_pymongo(crawler.crawl(depth=1), db.index)

This would allow us to execute more advanced query operations.
For example, to fetch all data for pressures greater than 2.0:

.. code-block:: python

    docs = db.index.find({'statepoint.p': {'$gt': 2.0}})
    for doc in docs:
        file = signac.fetch_one(doc)
        V = float(file.read())
        print(doc['statepoint'], V)

Integrating other tools
=======================

As a final chapter, we want to have a look at how we could integrate a non-python tool into our workflow.
Let's stick to the example and implement the ideal gas program in bash.
As bash can only evaluate expressions with integer values we need to express the pressure as a fraction and otherwise assume that *N* and *T* are integer values:

.. code-block:: bash

    # idg.sh
    N=$1
    T=$2
    p_num=$3        # bash expressions can only contain integers.
    p_denom=${4-1}  # The denominator defaults to 1.
    V=${expr $N \* $T \* $p_num / $p_denom}
    echo $V

We should now test our program on the command line:

.. code-block:: bash

   $ bash idg.sh 1000 1 1
   1000

There are many different ways on how to integrate this tool into our workflow.
One alternative would be to take advantage of signac's command line interface:

.. code-block:: bash

    $ signac job '{"N": 1000, "T": 1.0, "p": 1.0}'
    9e100da58ccdf6ad7941fce7d14deeb5

We could pipe the results of the computation into a file like this:

.. code-block:: bash

    $ bash idg.sh 1000 1 1 > `signac job -cw '{"N": 1000, "T": 1.0, "p": 1.0}'`/V.txt

Another alternative is to use a python script to prepare the execution of the other tool.
This has the additional advantage that we can use the :py:mod:`fractions` module to work-around bash's integer limitation:

.. code-block:: python

    # prepare_idg.py
    from fractions import Fraction
    import signac

    cmd = 'bash idg.sh {N} {T} {p_n} {p_d} > {out}'

    project = signac.get_project()
    for job in project.find_jobs():
        sp = job.statepoint()
        p = Fraction(sp['p'])
        print(cmd.format(
            N=int(sp['N']), T=int(sp['T']),
            p_n=p.numerator, p_d=p.denominator,
            out=job.fn('V.txt')))

This will generate a chain of one command for each state point in our data space:

.. code-block:: bash

    $ python prepare_idg.py
    bash idg.sh 1000 1 10 1 > ~/ideal_gas_project/workspace/07dc3f53615713900208803484b87253/V.txt
    bash idg.sh 1000 1 9 2 > ~/ideal_gas_project/workspace/14ba699529683f7132c863c51facc79c/V.txt
    # ...

To execute this we could simply pipe these commands into another bash script:

.. code-block:: bash

    $ python prepare_idg.py > run.sh
    $ bash run.sh
    $ # Or execute directly:
    $ python prepare_idg.py | bash


Further reading
===============

This concludes the tutorial.
To learn more about the individual components, check out the :ref:`guide` or inspect the :ref:`api` documentation.
A quick overview of the most important components are provided in the :ref:`quickreference`.
