# General

  * Implement a script to set up a mock project with folder hierarchy and minimal example scripts
  * Specialize project set up scripts for hoomd and cluster projects

# Documentation

  * Provide docstrings for all tested routines.
  * Provide minimal and extended example project
  * Provide a no python example project
  * Provide tutorial for example project
  * Provide a schematic diagram of the current implementation

# Debugging and testing

  * Provide a project-wide debug switch wich marks everything as debug runs.
  * Provide a routine to clean a project from debug jobs and data.
  * Provide a testing script which simulates a concurrent execution of the current setup.
  * Provide a good concept to provide debug and testing parameters, essentially different parameter sets

# Views

  * Provide script which generates a folder hierarchy based on a db query either by linking or copying.
  * Use the view scripts to generate 'backup scripts', that allow to  extract the data in case the database meta data is lost or no db is available.

# Global Database

  * Provide global db to store and load data from
  * Provide structure classes for the classified storage of data

# Jobs

  * Implement Milestone/ Jobcontrol concept
  * Consider to provide specialized job class, that handle the need for multiple runs with different randomseeds
  * Provide a routine to clean a project from jobs that have not reached a specific milestone
  * Discourage the extension of job scripts, better: Provide a new job context.
  * Provide the 'cached' method, see below.
  * Automatically store meta data with each job, see below.
  * Consider provide switch to not delete working directory (highly discouraged!!)
  * Provide a heart beat functionality for jobs

## Cached methods

The cached method allows to cache the in- and output of a routine in the global db.
It automatically stores the meta data of the routine to ensure that it has not changed.
This requires a smart way to store and compare python code for changes.

Example:
  
    with open_job(..) as job:
      # Executed or loaded from db, if available
      result = job.chached(my_routine, arg1, arg2, ...)  

      # Executed or file provided by db, if available
      job.chache(my_routine, outputfile = 'outputfile', arg1, arg2, ...)

Further along the road we should use this concept to provide standard routines for standardized data.

## Meta data
  
These are examples for meta-data, that should be stored automatically with each job.

  * a unique id per opening, used for
  * open/close/error protocol
  * git sha1 if available
  * signac config ?

## Heart beat

Start a heart beat process with each job opening, that pings the db server with a specific period.
This can be used to determine if a job is still alive.
Also used to clean the database from dead jobs.

# No python

  * Provide scripts that allow to open a job context and store data from the command line
  * Provide script to fetch from global db from the command line

# Analysis

  * Use hoomd json output
  * Enable the query of jobs not only for the job parameters, but also the content!

# Config
    
  * Search for config file with attribute 'project' recursively upwards!
