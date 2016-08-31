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
        statepoint = {'p': pressure, 'kT': 1.0, 'N': 1000}
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
    IdealGasProject
    665547b1344fe40de5b2c7ace4204783 06.7 init
    ee617ad585a90809947709a7a45dda9a 01.0 init,volume-computed
    b45a2485a44a46364cc60134360ea5af 04.5 init
    05061d2acea19d2d9a25ac3360f70e04 05.6 init
    c0ab2e09a6f878019a6057175bf718e6 02.3 init
    9110d0837ad93ff6b4013bae30091edd 03.4 init
    5a456c131b0c5897804a4af8e77df5aa 10.0 init,volume-computed
    e8186b9b68e18a82f331d51a7b8c8c15 08.9 init
    8629822576debc2bfbeffa56787ca348 07.8 init
    22582e83c6b12336526ed304d4378ff8 01.2 init
    5a6c687f7655319db24de59a2336eff8 00.1 init,volume-computed

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
    9110d0837ad93ff6b4013bae30091edd computed volume
    665547b1344fe40de5b2c7ace4204783 computed volume
    # ...


Determining the next operation
------------------------------

In an effort to modularize our workflow, we split the definition of operations and the code for execution into two different modules.
We move the ``calc_volume()`` and ``compute_volume()`` functions into an ``operations.py`` module:

.. code-block:: python

    # operations.py
    def calc_volume(N, kT, p):
        "Compute the volume of an ideal gas."
        return N * kT / p

    def compute_volume(job):
        "Compute the volume of this state point."
        sp = job.statepoint()
        with job:
            V = calc_volume(sp['N'], sp['kT'], sp['p'])
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
