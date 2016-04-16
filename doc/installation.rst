.. _installation:

Installation
============

With conda
----------

.. sidebar:: Closed-source

    **signac** is not yet publicly released, installation via conda therefore requires access to the *private* Glotzer channel.

To install the package with conda_, execute

.. code:: bash

    $ conda install signac

To upgrade, execute

.. code:: bash

    $ conda update signac

.. _conda: http://conda.pydata.org

.. note::

    This is the recommended installation method.

With pip
--------

To install the package with the package manager pip_, execute

.. _pip: https://docs.python.org/3.5/installing/index.html

.. code:: bash

    $ pip3 install git+https://$USER@bitbucket.org/glotzer/signac.git#egg=signac --user

.. note::
    It is highly recommended to install the package into the user space and not as superuser!

To upgrade the package, simply execute the same command with the `--upgrade` option.

.. code:: bash

    $ pip3 install git+https://$USER@bitbucket.org/glotzer/signac.git#egg=signac --user --upgrade

With git
--------

Alternatively you can clone the git repository and use the ``setup.py`` to install the package.

.. code:: bash

  git clone https://$USER@bitbucket.org/glotzer/signac.git
  cd signac
  python3 setup.py install --user
