.. _installation:

============
Installation
============

The recommendend installation method for **signac** is via conda_ or pip_.
The software is tested for python versions 2.7.x and 3.x and does not have any *hard* dependencies, i.e. there are no packages or libraries required to use the core **signac** functions.
However, some extra features, such as the database integration require additional packages.

.. _conda: https://anaconda.org/
.. _pip: https://docs.python.org/3.5/installing/index.html

Install with conda
==================

To install **signac** via conda, you first need to add the conda-forge_ channel with:

.. _conda-forge: https://conda-forge.github.io

.. code:: bash

    $ conda config --add channels conda-forge

Once the **conda-forge** channel has been enabled, **signac** can be installed with:

.. code:: bash

    $ conda install signac

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

Consider to install optional dependencies:

.. code:: bash

    $ pip install pymongo passlib bcrypt --user


Source Code Installation
========================

Alternatively you can clone the `git repository <https://bitbucket.org/glotzer/signac>`_ and execute the ``setup.py`` script to install the package.

.. code:: bash

  git clone https://bitbucket.org/glotzer/signac.git
  cd signac
  python setup.py install --user

Consider to install :ref:`optional dependencies <optional_dependencies>`.

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
