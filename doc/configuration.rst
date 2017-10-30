.. _configuration:

=============
Configuration
=============

Overview
========

The **signac** framework is configured with configuration files, which are named either ``.signacrc`` or ``signac.rc``.
These configuration files are searched for at multiple locations in the following order:

  1. in the current working directory,
  2. in each directory above the current working directory until a project configuration file is found,
  3. and the user's home directory.

The configuration file follows the standard "ini-style".
Global configuration options, should be stored in the home directory, while project-specific options should be stored *locally* in a project configuration file.

This is an example for a global configuration file in the user's home directory:

.. code-block:: ini

   # ~/.signarc
   [hosts]
   [[localhost]]
   url = mongodb://localhost

You can either edit these configuration files manually, or execute ``signac config`` on the command line.
Please see ``signac config --help`` for more information.

Project configuration
=====================

A project configuration file is defined by containing the keyword *project*.
Once **signac** found a project configuration file it will stop to search for more configuration files above the current working directory.

For example, to initialize a project named *MyProject*, navigate to the project's root directory and either execute ``$ signac init MyProject`` on the command line, use the :py:func:`signac.init_project` function or create the project configuration file manually.
This is an example for a project configuration file:

.. code-block:: ini

   # signac.rc
   project = MyProject
   workspace_dir = $HOME/myproject/workspace

project
  The name is required for the identification of the project's root directory.

workspace_dir
  The path to your project's workspace, which defaults to ``$project_root_dir/workspace``.
  Can be configured relative to the project's root directory or as absolute path and may contain environment variables.


Host configuration
==================

The current version of **signac** supports MongoDB databases as a backend.
To use **signac** in combination with a MongoDB database, make sure to install ``pymongo``.

Configuring a new host
----------------------

To configure a new MongoDB database host, create a new entry in the ``[hosts]`` section of the configuration file.
We can do so manually or by using the ``signac config host`` command.

Assuming that we a have a MongoDB database reachable via *example.com*, which requires a username and a password for login, execute:

.. code-block:: bash

    $ signac config host example mongodb://example.com -u johndoe -p
    Configuring new host 'example'.
    Password:
    Configured host 'example':
    [hosts]
    [[example]]
            url = mongodb://example.com
            username = johndoe
            auth_mechanism = SCRAM-SHA-1
            password = ***

The name of the configured host (here: *example*) can be freely chosen.
You can omit the ``-p/--password`` argument, in which case the password will not be stored and you will prompted to enter it for each session.

We can now connect to this host with:

.. code-block:: python

    >>> import signac
    >>> db = signac.get_database('mydatabase', hostname='example')

The ``hostname`` argument defaults to the first configured host and can always be omitted if there is only one configured host.

.. note::

    To prevent unauthorized users from obtaining your login credentials, **signac** will update the configuration file permissions such that it is only readable by yourself.


Changing the password
---------------------

To change the password for a configured host, execute

.. code-block:: bash

    $ signac host example --update-pw -p

.. warning::

    By default, any password set in this way will be **encrypted**. This means that the actual password is different from the one that you entered.
    However, while it is practically impossible to guess what you entered, a stored password hash will give any intruder access to the database.
    This means you need to **treat the hash like a password!**

Copying a configuration
-----------------------

In general, in order to copy a configuration from one machine to another, you can simply copy the ``.signacrc`` file as is.
If you only want to copy a single host configuration, you can either manually copy the associated section or use the ``signac config host`` command for export:

.. code-block:: bash

    $ signac config host example > example_config.rc

Then copy the ``example_config.rc`` file to the new machine and rename or append it to an existing ``.signacrc`` file.
For security reasons, any stored password is not directly copied in this way.
To copy the password, follow:

.. code-block:: bash

    # Copy the password from the old machine:
    johndoe@oldmachine $ signac config host example --show-pw
    XXXX
    # Enter it on the new machine:
    johndoe@newmachine $ signac config host example -p


Manual host configuration
-------------------------

You can configure one or multiple hosts in the ``[hosts]`` section, where each subsection header specifies the host's name.

url
  The url specifies the MongoDB host url, e.g. ``mongodb://localhost``.
authentication_method (default=none)
  Specify the authentication method with the database, possible choices are: ``none`` or ``SCRAM-SHA-1``.
username
  A username is required if you authenticate via ``SCRAM-SHA-1``.
password
  The password to authenticate via ``SCRAM-SHA-1``.
db_auth (default=admin)
  The database to authenticate with.
password_config
  In case that you update, but not store your password, the configuration file will contain only meta hashing data, such as the salt.
  This allows to authenticate by entering the password for each session, which is generally more secure than storing the actual password hash.

.. warning::

    **signac** will automatically change the file permissions of the configuration file to *user read-write only* in case that it contains authentication credentials.
    In case that this fails, you can set the permissions manually, e.g., on UNIX-like operating systems with: ``chmod 600 ~/.signacrc``.
