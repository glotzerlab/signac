# Computational Database

## About

The Computational Database (CompDB) aids in the management, access and analysis of large-scale computational investigations.
The framework provides a simple data model, which helps to organize data production and post-processing as well as distribution among collaboratos.

## Installation

The recommeded method of installation is using *pip*:

    $ pip3 install git+https://$USER@bitbucket.org/glotzer/compdb.git#egg=compdb --user

To upgrade, the package, simply append the `--upgrade` option to this command.
For more information on installation, upgrade and removal, please see the [wiki](https://bitbucket.org/glotzer/compdb/wiki/latest/Setup).

## Testing

To check if the package was installed correctly, execute `import compdb` within a python shell.
That should not result in any error.

Executing `compdb` on the command line should produce similar output to:

    $ compdb
    usage: compdb [-h] [-y] [-v]
                  {init,config,remove,snapshot,restore,cleanup,info,view,check,server,log}
                  ...

If the command above fails, please refer to [here](https://bitbucket.org/glotzer/compdb/wiki/set_path).

For detailed testing, execute `nosetests` within the repositories root directory.
Most tests require a MongoDB instance to connect to. The default is 'localhost'. To specify a different server host, execute:

    $ compdb config set database_host yourhost.com

## Quickstart

The framework facilitates a project-based workflow.
Setup a new project:

    $ mkdir my_project
    $ cd my_project
    $ compdb init MyProject

This will create the basic configuration for a project named "MyProject" within the directory `my_project`.
In addition, a few example scripts will be created, that may, but do not have to be the starting point for the creation of new project routines.

To test if everything is correctly setup, you can then execute `$ compdb check` which will check your configuration, the connectivity and permissions on the database.

Add a job to the job queue:

    project.job_queue.submit(my_job, state_point)

Execute jobs from the queue in serial:

    $ compdb server

and in parallel:

    $ mpirun -np 8 compdb server

## Get help:

The tutorial and receipts are located in the [wiki](https://bitbucket.org/glotzer/compdb/wiki).
Checkout the examples in the example folder.