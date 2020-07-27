.. _cli:

======================
Command Line Interface
======================

.. rubric:: Overview

.. autosummary::

    signac init
    signac project

The following core **signac** functions are---in addition to the Python interface---accessible
directly via the ``$ signac`` command.


init
``$ signac init [-h] [-w WORKSPACE] project_id``
    positional arguments:
    project_id            Initialize a project with the given project id.

    optional arguments:
    -h, --help            show this help message and exit
    -w WORKSPACE, --workspace WORKSPACE
                            The path to the workspace directory.

project
``$ signac project [-h] [-w] [-i] [-a]``
    optional arguments:
    -h, --help       show this help message and exit
    -w, --workspace  Print the project's workspace path instead of the project id.
    -i, --index      Generate and print an index for the project.
    -a, --access     Create access module for indexing.

``$ signac job``

``$ signac statepoint``

``$ signac diff``

``$ signac document``

``$ signac rm``

``$ signac move``

``$ signac clone``

``$ signac index``

``$ signac find``

``$ signac view``

``$ signac schema``

``$ signac shell``

``$ signac sync``

``$ signac import``

``$ signac export``