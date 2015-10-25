Installation
============

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

Alternatively you can clone the git repository.

.. code:: bash

  git clone https://$USER@bitbucket.org/glotzer/signac.git
  cd signac
  python3 setup.py install --user
