.. _support:

=======================
Support and Development
=======================

To get help using the **signac** package, either send an email to `signac-support@umich.edu <mailto:signac-support@umich.edu>`_ or join the `signac gitter chatroom <https://gitter.im/signac/Lobby>`_.

The **signac** package is hosted on `GitHub <https://github.com/glotzerlab/signac.git>`_ and licensed under the open-source BSD 3-Clause license.
Please use the `repository's issue tracker <https://github.com/glotzerlab/signac/issues>`_ to report bugs or request new features.


Code contributions
==================

This project is open-source.
Users are highly encouraged to contribute directly by implementing new features and fixing issues.
Development for packages as part of the **signac** framework should follow the general development guidelines outlined `here <http://docs.signac.io/en/latest/community.html#contributions>`_.

A brief summary of contributing guidelines are outlined in the `CONTRIBUTING.md <https://github.com/glotzerlab/signac/blob/master/CONTRIBUTING.md>`_ file as part of the repository.
All contributors must agree to the `Contributor Agreement <https://github.com/glotzerlab/signac/blob/master/ContributorAgreement.md>`_ before their pull request can be merged.

Set up a development environment
--------------------------------

Start by `forking <https://github.com/glotzerlab/signac/fork/>`_ the project.

We highly recommend to setup a dedicated development environment,
for example with `venv <https://docs.python.org/3/library/venv.html>`_:

.. code-block:: bash

    ~ $ python -m venv ~/envs/signac-dev
    ~ $ source ~/envs/signac-dev/bin/activate
    (signac-dev) ~ $ pip install six flake8

or alternatively with `conda <https://conda.io/docs/>`_:

.. code-block:: bash

    ~ $ conda create -n signac-dev python=3 six flake8
    ~ $ activate signac-dev

Then clone your fork and install the package from source with:

.. code-block:: bash

    (signac-dev) ~ $ cd path/to/my/fork/of/signac
    (signac-dev) signac $ pip install -e . -r requirements-dev.txt

The ``-e`` option stands for *editable*, which means that the package is directly loaded from the source code repository.
That means any changes made to the source code are immediately reflected upon reloading the Python interpreter.

Finally, we recommend to setup a `Flake8 <http://flake8.pycqa.org/en/latest/>`_ git commit hook with:

.. code-block:: bash

    (signac-dev) signac $ flake8 --install-hook git
    (signac-dev) signac $ git config --bool flake8.strict true

With the *flake8* hook, your code will be checked for syntax and style before you make a commit.
The continuous integration pipeline for the package will perform these checks as well, so running these tests before committing / pushing will prevent the pipeline from failing due to style-related issues.

The development workflow
------------------------

Prior to working on a patch, it is advisable to create an `issue <https://github.com/glotzerlab/signac/issues>`_ that describes the problem or proposed feature.
This means that the code maintainers and other users get a chance to provide some input on the scope and possible limitations of the proposed changes, as well as advise on the actual implementation.

All code changes should be developed within a dedicated git branch and must all be related to each other.
Unrelated changes, such as minor fixes to unrelated bugs encountered during implementation, spelling errors, and similar typographical mistakes must be developed within a separate branch.

Branches should be named after the following pattern: ``<prefix>/issue-<#>-optional-short-description``.
Choose from one of the following prefixes depending on the type of change:

  * ``fix/``: Any changes that fix the code and documentation.
  * ``feature/``: Any changes that introduce a new feature.
  * ``release/``: Reserved for release branches.

If your change does not seem to fall into any of the above mentioned categories, use ``misc/``.

Once you are content with your changes, push the new branch to your forked repository and create a pull request into the main repository.
Feel free to push a branch before completion to get input from the maintainers and other users, but make sure to add a comment that clarifies that the branch is not ready for merge yet.

Testing
-------

Prior to fixing an issue, implement unit tests that *fail* for the described problem.
New features must be tested with unit and integration tests.
To run tests, execute:

.. code-block:: bash

    (signac-dev) signac $ pytest


Building documentation
----------------------

Building documentation requires the `sphinx <http://www.sphinx-doc.org/en/master/>`_ package which you will need to install into your development environment.

.. code-block:: bash

   (signac-dev) signac $ pip install Sphinx sphinx_rtd_theme

Then you can build the documentation from within the ``doc/`` directory as part of the source code repository:

.. code-block:: bash

    (signac-dev) signac $ cd doc/
    (signac-dev) doc $ make html

.. note::

    Documentation as part of the package should be largely limited to the API.
    More elaborate documentation on how to integrate **signac** into a computational workflow should be documented as part of the `framework documentation <https://docs.signac.io>`_, which is maintained `here <https://github.com/glotzerlab/signac-docs>`_.


Updating the changelog
----------------------

To update the changelog, add a one-line description to the `changelog.txt <https://github.com/glotzerlab/signac/blob/master/changelog.txt>`_ file within the ``next`` section.
For example:

.. code-block:: bash

    next
    ----

    - Fix issue with launching rockets to the moon.

    [0.6.3] -- 2018-08-22
    ---------------------

    - Fix issue related to dynamic data spaces, ...

Just add the ``next`` section in case it doesn't exist yet.
