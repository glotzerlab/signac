.. _grouping:

=========
Grouping
=========

Grouping operations can be performed on data spaces or the results of search queries, enabling analysis of multiple jobs or state points together.

The :py:class:`~.JobsCursor` class represents search results, and its contents can be grouped using state point parameters, job document values, or arbitrary lambda functions.

Basics
======

Grouping can be quickly performed using a statepoint or job document key.

If *a* was a state point variable in a project's parameter space, we can quickly enumerate the groups corresponding to each value of *a* like this:

.. code-block:: python

    for key, group in project.groupby('a'):
        print(key, list(group))

Similarly, we can group by values in the job document as well. Here, we group all jobs in the project by a job document key *b*:

.. code-block:: python

    for key, group in project.groupbydoc('b'):
        print(key, list(group))

Multiple Groupings
==================

Grouping by multiple state point parameters or job document values is possible, by passing an iterable of fields that should be used for grouping. For example, we can group jobs by state point parameters *c* and *d*:

.. code-blocK:: python

    for key, group in project.groupby(('c', 'd')):
        print(key, list(group))

Chained Search and Grouping
===========================

Together with the *signac* searching features, grouping becomes more useful. We can find all jobs where ``job.sp['e']`` is 1 and then group them by state point parameter *f*:

.. code-block:: python

    for key, group in project.find_jobs({'e': 1}).groupby('f'):
        print(key, list(group))

Custom Grouping Expressions
===========================

The use of ``lambda`` expressions allows for jobs to be grouped in nearly any way.
The :py:meth:`~.JobsCursor.groupby` and :py:meth:`~.JobsCursor.groupbydoc` methods accept lambda expressions of one argument, the ``job`` or ``job.document``, respectively.

By utilizing functions, we may group jobs in arbitrary ways. For example, we may group by a combination of state point and document values:

.. code-block:: python

    for key, group in project.groupby(
        lambda job: (job.sp['d'], job.document['count'])
    ):
        print(key, list(group))
