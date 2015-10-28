# signac - database-driven simulation management

## About

signac aids in the management, access and analysis of large-scale computational investigations.
The framework provides a simple data model, which helps to organize data production and post-processing as well as distribution among collaborators.

## Installation

The recommeded method of installation is using *pip*:

    pip3 install git+https://$USER@bitbucket.org/glotzer/signac.git#egg=signac --user

To upgrade, the package, simply append the `--upgrade` option to this command.

Detailed information about installing and configuring this package can be found in the documentation.

## Documentation

You can download the documentation for the lastest release in [html](https://bitbucket.org/glotzer/signac/downloads/signac-documentation.tar.gz) or [pdf](https://bitbucket.org/glotzer/signac/downloads/signac.pdf) format.

To build documentation for the latest version with [sphinx](http://sphinx-doc.org), execute:

    $ git clone https://$USERf@bitbucket.org/glotzer/signac.git
    $ cd signac
    $ python3 setup.py install --user
    $ cd doc
    $ make html

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