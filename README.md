# signac - simple data management

[![DOI](https://zenodo.org/badge/72946496.svg)](https://zenodo.org/badge/latestdoi/72946496)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/signac/badges/version.svg)](https://anaconda.org/conda-forge/signac)
[![PyPi](https://img.shields.io/pypi/v/signac.svg)](https://img.shields.io/pypi/v/signac.svg)
[![Builds](https://pipelines-badges-service.useast.staging.atlassian.io/badge/glotzer/signac.svg)](https://coveralls.io/bitbucket/glotzer/signac?branch=master)
[![License](https://img.shields.io/github/license/csadorf/signac.svg)](https://bitbucket.org/glotzer/signac/src/master/LICENSE.txt)
[![Binder](http://mybinder.org/badge.svg)](http://www.mybinder.org:/repo/csadorf/signac-examples)

## About

The signac framework aids in the management of large and heterogeneous data spaces.

It provides a simple and robust data model to create a well-defined indexable storage layout for data and metadata.
This makes it easier to operate on large data spaces, streamlines post-processing and analysis and makes data collectively accessible.

**The package's documentation is available at: [http://signac.readthedocs.io](http://signac.readthedocs.io)**

## Installation

The recommendend installation method for **signac** is through **conda** or **pip**.
The software is tested for python versions 2.7.x and 3.x and is built for all major platforms.

### Install via **conda**

Installing **signac** from the **conda-forge** channel can be achieved by adding **conda-forge** to your channels with:

    conda config --add channels conda-forge

Once the **conda-forge** channel has been enabled, **signac** can be installed with:

    conda install signac

### Install via **pip**

To install **signac** via **pip**, execute:

    pip install --user signac

**Detailed information about alternative installation methods and configuration of this package can be found in the [documentation](https://signac.readthedocs.io/en/latest/installation.html).**

## Quickstart

The framework facilitates a project-based workflow.
Setup a new project:

    $ mkdir my_project
    $ cd my_project
    $ signac init MyProject

and access the project handle:

    >>> project = signac.get_project()

## Documentation

The documentation is available **online** at [https://signac.readthedocs.io](https://signac.readthedocs.io).
We further invite you to check out the collection of [Jupyter notebooks](https://bitbucket.org/glotzer/signac-examples), which serve both as **tutorial** and **examples collection**.

The **tutorial** is also hosted [online](http://www.mybinder.org:/repo/csadorf/signac-examples) as a free service by courtesy of [mybinder.org](http://www.mybinder.org).
Please note, that the online hosting of notebooks is not always available.

## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately. We have prepared examples on how to do that [here](http://signac.readthedocs.io/en/latest/acknowledge.html). **Thank you very much!**
