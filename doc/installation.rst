.. _installation:

============
Installation
============

Install with conda
==================

To install the package with conda_, execute

.. code:: bash

    $ conda install -c glotzer signac

To upgrade, execute

.. code:: bash

    $ conda update signac

.. _conda: http://conda.pydata.org

.. note::

    This is the recommended installation method.

Alternative installation methods
================================

Optional dependencies
---------------------

When using one of the alternative installation  methods, any optional dependencies may not get automatically installed.
In the case that you want to use extra features that requires dependencies, you need to install those manually.

Extra features with dependencies:

.. glossary::

    MongoDB database backend
      required: ``pymongo``

      recommended: ``passlib``, ``bcrypt``

    Graphical User Interface (GUI)
      required: ``PySide``

Install with pip
----------------

To install the package with the package manager pip_, execute

.. _pip: https://docs.python.org/3.5/installing/index.html

.. code:: bash

    $ pip install signac --user

.. note::
    It is highly recommended to install the package into the user space and not as superuser!

To upgrade the package, simply execute the same command with the `--upgrade` option.

.. code:: bash

    $ pip install signac --user --upgrade

Consider to install optional dependencies:

.. code:: bash

    $ pip install pymongo passlib bcrypt

Install with git
----------------

Alternatively you can clone the `git repository <https://bitbucket.org/glotzer/signac>`_ and use the ``setup.py`` script to install the package.

.. code:: bash

  git clone https://$USER@bitbucket.org/glotzer/signac.git
  cd signac
  python setup.py install --user

Consider to install optional dependencies (see above).
