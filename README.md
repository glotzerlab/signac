# Computational Materials database

## Requirements
  
The computational material's database needs to connect to a MongoDB instance as database backend.
You can obtain a copy at [MongoDB](http://www.mongodb.org).
You will also need the python MongoDB driver `pymongo`, which should be installed automatically.
If not try `pip install pymongo` to install the package.

The package requires Python 3 and is tested with version 3.3.

## Installation

    $ git clone git@bitbucket.org:glotzer/compdb.git
    $ cd compdb
    $ python setup.py install

On system's without root access you can install the package with
  
    $ python setup.py install --user

into your home directory.

## Testing

To check if the package was installed correctly, execute `import compdb` within a python shell.
That should not result in any error.

To test the package, execute `nosetests` within the repositories root directory.
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

To test if everything is correctly setup, you can then execute `$ compdb check` which will check your configuration and the connectivity to the configured database.

Add a job to the job queue:

    project.job_queue.submit(my_job, state_point)

Execute jobs from the queue in serial:

    $ compdb run

and in parallel:

    $ mpirun -np 8 compdb run

## Get help:

The tutorial and receipts are located in the [wiki](https://bitbucket.org/glotzer/compdb/wiki).
Checkout the examples in the example folder.