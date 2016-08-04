.. _tutorial_workflow:
A complete Workflow
===================

Job Classification
------------------

Let's imagine we are still not convinced of the pressure-volume relationship that we "discovered" and want to add a few more state points.
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

In an effort to modularize our workflow, we split the definition of operations and the code for execution into two different modules.
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
