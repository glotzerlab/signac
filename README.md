# signac - a simple data management framework

## About

The signac framework aids in the management of large and heterogeneous data spaces.

It provides a simple and robust data model to create a well-defined indexable storage layout for data and metadata.
This makes it easier to operate on large data spaces, streamlines post-processing and analysis and makes data collectively accessible.

**The package's documentation is available at: [http://signac.readthedocs.io/en/latest/](http://signac.readthedocs.io/en/latest/)**

## Installation

The recommeded method of installation is using *conda*:

    conda install -c glotzer signac

To upgrade, the package, execute:

    conda update -c glotzer signac

**Detailed information about alternative installation methods and configuration of this package can be found in the [documentation](https://glotzerlab.engin.umich.edu/signac/installation.html).**

## Quickstart

The framework facilitates a project-based workflow.
Setup a new project:

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject
    # or
    $ echo project=MyProject >> signac.rc

and access the project handle:
   
    >>> project = signac.get_project()

To access a database:

    >>> db = signac.get_database('MyDatabase')

## Documentation

Documentation for the current master branch is available at [https://glotzerlab.engin.umich.edu/signac](https://glotzerlab.engin.umich.edu/signac).

To build documentation yourself with [sphinx](http://sphinx-doc.org), clone the repository, install the package and then execute:

    $ cd doc
    $ make html

from within the repository's root directory.