.. _installation:

============
Installation
============

The recommended installation method for **signac** is via conda_ or pip_.
The software is tested for Python versions 2.7.x and 3.4+ and does not have any *hard* dependencies, i.e. there are no packages or libraries required to use the core **signac** functions.
However, some extra features, such as the database integration require additional packages.

.. _conda: https://conda.io/
.. _conda-forge: https://conda-forge.org/
.. _pip: https://pip.pypa.io/en/stable/

Install with conda
==================

You can install **signac** via conda (available on the conda-forge_ channel), with:

.. code:: bash

    $ conda install -c conda-forge signac

All additional dependencies will be installed automatically.
To upgrade the package, execute:

.. code:: bash

    $ conda update signac


Install with pip
================

To install the package with the package manager pip_, execute

.. code:: bash

    $ pip install signac --user

.. note::
    It is highly recommended to install the package into the user space and not as superuser!

To upgrade the package, simply execute the same command with the ``--upgrade`` option.

.. code:: bash

    $ pip install signac --user --upgrade

Consider installing optional dependencies:

.. code:: bash

    $ pip install pymongo passlib bcrypt --user


Source Code Installation
========================

Alternatively you can clone the `git repository <https://bitbucket.org/glotzer/signac>`_ and execute the ``setup.py`` script to install the package.

.. code:: bash

  git clone https://bitbucket.org/glotzer/signac.git
  cd signac
  python setup.py install --user

Consider installing :ref:`optional dependencies <optional_dependencies>`.

.. _optional_dependencies:

Optional dependencies
=====================

Unless you install via conda_, optional dependencies are not installed automatically.
In case you want to use extra features that require external packages, you need to install these manually.

Extra features with dependencies:

.. glossary::

    MongoDB database backend
      required: ``pymongo``

      recommended: ``passlib``, ``bcrypt``
