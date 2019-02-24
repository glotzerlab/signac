# <img src="doc/images/logo.png" width="100" height="100"> Signac - Simple Data Management

[![DOI](https://zenodo.org/badge/72946496.svg)](https://zenodo.org/badge/latestdoi/72946496)
[![PyPI](https://img.shields.io/pypi/v/signac.svg)](https://pypi.org/project/signac/)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/signac.svg?style=flat)](https://anaconda.org/conda-forge/signac)
![CircleCI](https://img.shields.io/circleci/project/github/glotzerlab/signac/master.svg)
[![RTD](https://img.shields.io/readthedocs/signac.svg?style=flat)](https://docs.signac.io)
[![License](https://img.shields.io/github/license/csadorf/signac.svg)](https://github.com/glotzerlab/signac/blob/master/LICENSE.txt)
[![PyPI-downloads](https://img.shields.io/pypi/dm/signac.svg?style=flat)](https://pypistats.org/packages/signac)
[![conda-forge-downloads](https://img.shields.io/conda/dn/conda-forge/signac.svg)](https://anaconda.org/conda-forge/signac)
[![Gitter](https://img.shields.io/gitter/room/signac/Lobby.svg?style=flat)](https://gitter.im/signac/Lobby)


The [signac framework](https://signac.io) aids in the management of large and heterogeneous data spaces.

It provides a simple and robust data model to create a well-defined indexable storage layout for data and metadata.
This makes it easier to operate on large data spaces, streamlines post-processing and analysis and makes data collectively accessible.

**The documentation is available at: [https://docs.signac.io](https://docs.signac.io)**

## Installation

The recommended installation method for **signac** is through **conda** or **pip**.
The software is tested for Python versions 2.7.x and 3.x and is built for all major platforms.

To install **signac** *via* the [conda-forge](https://conda-forge.github.io/) channel, execute:

    conda install -c conda-forge signac

To install **signac** *via* **pip**, execute:

    pip install signac

**Detailed information about alternative installation methods can be found in the [documentation](https://docs.signac.io/en/latest/installation.html).**

## Quickstart

The framework facilitates a project-based workflow.
Setup a new project:

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject

and access the project handle:

    >>> project = signac.get_project()

## Documentation

The documentation is hosted at [https://docs.signac.io](https://docs.signac.io).

## Testing

You can test this package by executing:

    $ python -m unittest discover tests/

## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately.
We have prepared examples on how to do that [here](http://docs.signac.io/en/latest/acknowledge.html).
**Thank you very much!**
