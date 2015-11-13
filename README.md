# signac - database-driven simulation management

## About

signac aids in the management, access and analysis of large-scale computational investigations.
The framework provides a simple data model, which helps to organize data production and post-processing as well as distribution among collaborators.

**The package's documentation is available at: [https://glotzerlab.engin.umich.edu/signac](https://glotzerlab.engin.umich.edu/signac)**

## Installation

The recommeded method of installation is using *pip*:

    pip3 install git+https://$USER@bitbucket.org/glotzer/signac.git#egg=signac --user

To upgrade, the package, simply append the `--upgrade` option to this command.

**Detailed information about alternative installation methods and configuration of this package can be found in the [documentation](https://glotzerlab.engin.umich.edu/signac).**

## Documentation

Documentation for the current master branch is available at [https://glotzerlab.engin.umich.edu/signac](https://glotzerlab.engin.umich.edu/signac).

To build documentation yourself with [sphinx](http://sphinx-doc.org), clone the repository, install the package and then execute:

    $ cd doc
    $ make html

from within the repository's root directory.

## Quickstart

The framework facilitates a project-based workflow.
Setup a new project:

    $ mkdir my_project
    $ cd my_project
    $ echo project=MyProject >> signac.rc

and access the project handle
   
    >>> project = signac.contrib.get_project()

To access a database:

    >>> db = signac.db.get_database('MyDatabase')