.. _query:

=========
Query API
=========

As briefly described in :ref:`project-job-finding`, the :py:meth:`~.Project.find_jobs()` method provides much more powerful search functionality beyond simple selection of jobs with specific state point values.
More generally, all **find()** functions within the framework accept filter arguments that will return a selection of jobs or documents.
One of the key features of *signac* is the possibility to immediately search managed data spaces to select desired subsets as needed.
Internally, all search operations are processed by an instance of :py:class:`~.Collection` (see :ref:`collections`).
Therefore, they all follow the same syntax, so you can use the same type of filter arguments in :py:meth:`~.Project.find_jobs`, :py:meth:`~.Project.find_statepoints`, and so on.

.. note::

    The **signac** framework query API is a subset of the `MongoDB query API <https://docs.mongodb.com/manual/tutorial/query-documents/>`_!

Basics
======

Filter arguments are a mapping of expressions, where a single expression consists of a key-value pair.
All selected documents must match these expressions.

The simplest expression is an *exact match*.
For example, in order to select all jobs whose state point key *a* has the value 42, you would use the following expression: ``{'a': 42}`` as follows:

.. code-block:: python

    project.find_jobs({'a': 42})

Select All
----------

If you want to select the complete data set, don't provide any filter argument at all.
The default argument of ``None`` or an empty expression ``{}`` will select all jobs or documents.
As was previously demonstrated, iterating over all jobs in a project or all documents in a collection can be accomplished directly without using any *find* method at all:

.. code-block:: python

    for job in project:
        pass

    for doc in collection:
        pass

.. _simple-selection:

Simple Selection
----------------

To select documents by one or more specific key-value pairs, simply provide these directly as filter arguments.
For example, assuming that we have a list of documents with values *N*, *kT*, and *p*, as such:

.. code-block:: python

    1: {'N': 1000, 'kT': 1.0, 'p': 1}
    2: {'N': 1000, 'kT': 1.2, 'p': 2}
    3: {'N': 1000, 'kT': 1.3, 'p': 3}
    ...

We can select the 2nd document with ``{'p': 2}``, but also ``{'N': 1000, 'p': 2}`` or any other matching combination.

.. _nested-keys:

Nested Keys
-----------

To match **nested** keys, avoid nesting the filter arguments, but instead use the `.`-operator.
For example, if the documents shown in the example above were all nested like this:

.. code-block:: python

    1: {'statepoint': {'N': 1000, 'kT': 1.0, 'p': 1}}
    2: {'statepoint': {'N': 1000, 'kT': 1.2, 'p': 2}}
    3: {'statepoint': {'N': 1000, 'kT': 1.3, 'p': 3}}
    ...

Then we would use ``{'statepoint.p': 2}`` instead of ``{'statepoint': {'p': 2}}`` as filter argument.
This is not only easier to read, but also increases compatibility with MongoDB database systems.

Operator Expressions
====================

In addition to simple exact value matching, **signac** also provides **operator-expressions** to execute more complicated search queries.

.. _arithmetic-operators:

Arithmetic Expressions
----------------------

If we wanted to match all documents where *p is greater than 2*, we would use the following filter argument:

.. code-block:: python

    {'p': {'$gt': 2}}

Note that we have replaced the value for p with the expression ``{'$gt': 2}`` to select *all all jobs withe p values greater than 2*.
Here is a complete list of all available **arithmetic operators**:

  * ``$eq``: equal to
  * ``$neq``: not equal to
  * ``$gt``: greater than
  * ``$gte``: greater or equal than
  * ``$lt``: less than
  * ``$lte``: less or equal than

.. _near-operator:

Near Operator
-------------
The ``$near`` operator is used to find jobs with state point parameters that are near a value, where floating point precision may make it difficult to type the exact value.
The behavior of ``$near`` matches that of python's `math.isclose <https://docs.python.org/3.5/library/math.html/>`_ function.
The "reference" value and tolerances are passed in as a list in the order ``[reference, [relative_tolerance, [absolute_tolerance]]]``, where the inner ``[]``\s denote optional values.
Note that default values are ``relative_tolerance = 1e-09`` and ``absolute_tolerance = 0``.

.. code-block:: python

    signac find theta.\$near 0.6  # easier than typing 0.600000001
    signac find '{"p.$near": [100, 0.05]}'  # p within 5% of 100
    signac find '{"p.$near": [100, 0.05, 2]}'  # abs(p-100)/max(p, 100) < 0.05 or abs(p-100) < 2

.. _logical-operators:

Logical Operators
-----------------

There are two supported logical operators: ``$and`` and ``$or``.
To querying with a logical expression, we construct a mapping with the logical-operator as the key and a list of expressions as the value.
As usual, the ``$and`` operator matches documents where all the expressions are true, while the ``$or`` expression matches if any documents satisfy the provided expression.
For example, we can match all documents where *p is greater than 2* **or** *kT=1.0* we could use the following (split onto multiple lines for clarity):

.. code-block:: python

    {
       '$or': [
                {'p': {'$gt': 2}},    # either match this
                {'kT': 1.0}           # or this
              ]
    }

Logical expressions may be nested, but cannot be the *value* of a key-value expression.

.. _exists-operator:

Exists Operator
---------------

If you want to check for the existence of a specific key but do not care about its actual value, use the ``$exists``-operator.
For example, the expression ``{'p': {'$exists': True}}`` will return all documents that *have a key p* regardless of its value.
Likewise, using ``False`` as argument would return all documents that have no key with the given name.

.. _array-operator:

Array Operator
--------------

This operator may be used to determine whether specific keys have values, that are **in** (``$in``), or **not in** (``$nin``) a given array, e.g.:

.. code-block:: python

    {'p': {'$in': [1, 2, 3]}}

This would return all documents where the value for *p* is either 1, 2, or 3.
The usage of ``$nin`` is equivalent, and will return all documents where the value is *not in* the given array.

.. _regex-operator:

Regular Expression Operator
---------------------------

This operator may be used to search for documents where the value of type ``str`` matches a given *regular expression*.
For example, to match all documents where the value for *protocol* contains the string "assembly", we could use:

.. code-block:: python

    {'protocol': {'$regex': 'assembly'}}

This operator internally applies the :py:func:`re.search` function and will never match if the value is not of type ``str``.

.. _type-operator:

Type Operator
-------------

This operator may be used to search for documents where the value is of a specific type.
For example, to match all documents, where the value of the key *N* is of integer-type, we would use:

.. code-block:: python

    {'N': {'$type': 'int'}}

Other supported types include *float*, *str*, *bool*, *list*, and *null*.

.. _where-operator:

Where Operator
--------------

This operator allows us to apply a *custom function* to each value and select based on its return value.
For example, instead of using the regex-operator, as shown above, we could write the following expression:

.. code-block:: python

    {'protocol': {'$where': 'lambda x: "assembly" in x'}}


.. _simplified-filter:

Simplified Syntax on the Command Line
=====================================

It is possible to use search expressions directly on the command line, for example in combination with the ``$ signac find`` command.
In this case filter arguments are expected to be provided as valid JSON expressions.
However, for simple filters you can also use a *simplified syntax*.
For example, instead of ``{'p': 2}``, you can simply type ``p 2``.

A simplified expression consists of key-value pairs in alternation.
The first argument will then be interpreted as the first key, the second argument as the first value, the third argument as the second key, and so on.
If you provide an odd number of arguments, the last value will default to ``{'$exists': True}``.
Querying via operator is supported using the `.`-operator.
Finally, you can use ``/<regex>/`` intead of ``{'$regex': '<regex>'}`` for regular expressions.

The following list shows simplified expressions on the left and their equivalent standard expression on the right.

.. code-block:: python

    simplified            standard
    --------------------  ------------------------------------

    p                     {'p': {'$exists': True}}
    p 2                   {'p': 2}
    p 2 kT                {'p': 2, 'kT': {'$exists': True}}
    p 2 kT.$gte 1.0       {'p': 2, 'kT': {'$gte': 1.0}}
    protocol /assembly/   {'protocol': {'$regex': 'assembly'}}

.. important::

    The ``$`` character used in operator-expressions must be escaped in many terminals, that means for example instead of ``$ signac find p.$gt 2``, you would need to write ``$ signac find p.\$gt 2``.
