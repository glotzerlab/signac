=============
Configuration
=============

Signac is designed to be used with a MongoDB database as backend.
Use signac-gui_ to configure one or more MongoDB database hosts.
Alternatively you can also create configuration files manually.

.. _signac-gui: https://bitbucket.org/glotzer/signac-gui 

Manual configuration
====================

signac is configured with configuration files, which are named either `.signacrc` or `signac.rc`.
These configuration files are searched for at multiple locations in the following order:

  1. in the current working directory,
  2. in each directory above the current working directory until a project configuration file is found
  3. and the user's home directory.

The configuration file follows the standard "ini-style".

Example
-------

This is an example for a user configuration file:

.. code:: ini
   
   # ~/.signarc
   [hosts]
   [[localhost]]
   url=localhost

Host configuration
------------------

You can configure one or multiple hosts in the `[hosts]` section, where each subsection header specifies the hosts name.

.. warning::
   Change the file permissions of your configuration file to user read-write only in case that you need to store authentication credentials within your configuration file.

   In UNIX-like operating systems this is accomplished with: `chmod 600 .signacrc`

url
  The url specifies the MongoDB host url, e.g. `localhost`.
authentication_method (default=none)
  Specify the authentication method with the database, possible choices are: `none`, `SCRAM-SHA-1`, `SSL-x509` and `SSL`.
username
  A username is required if you authenticate via `SCRAM-SHA-1`.
password
  The password to authenticate via `SCRAM-SHA-1`.
  You need to change the file permissions of your configuration file to user read-write only for signac to read your password from the configuration file.
db_auth (default=admin)
  The database to authenticate with.

Project configuration
---------------------

A project configuration file is defined by containing the keyword `project`.
Once signac found a project configuration file it will stop to search for more configuration files above the current working directory.

This is an example for a project configuration file:

.. code:: ini

   # signac.rc
   project=MyProject
   workspace_dir=/home/johndoe/myproject/workspace

project
  The name is required for the identification of the project's root directory.
 
workspace_dir
  The path to your project's workspace.
  The workspace needs to be specified to make full use of signac's project management capabilities.

.. note::

  It is recommended to use an absolute path to specify the workspace directory.
