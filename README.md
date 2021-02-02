# <img src="https://raw.githubusercontent.com/glotzerlab/signac/master/doc/images/palette-header.png" width="75" height="58"> signac - simple data management

[![Affiliated with NumFOCUS](https://img.shields.io/badge/NumFOCUS-affiliated%20project-orange.svg?style=flat&colorA=E1523D&colorB=007D8A)](https://numfocus.org/sponsored-projects/affiliated-projects)
[![PyPI](https://img.shields.io/pypi/v/signac.svg)](https://pypi.org/project/signac/)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/signac.svg?style=flat)](https://anaconda.org/conda-forge/signac)
![CircleCI](https://img.shields.io/circleci/project/github/glotzerlab/signac/master.svg)
[![RTD](https://img.shields.io/readthedocs/signac.svg?style=flat)](https://docs.signac.io)
[![License](https://img.shields.io/github/license/glotzerlab/signac.svg)](https://github.com/glotzerlab/signac/blob/master/LICENSE.txt)
[![PyPI-downloads](https://img.shields.io/pypi/dm/signac.svg?style=flat)](https://pypistats.org/packages/signac)
[![Gitter](https://img.shields.io/gitter/room/signac/Lobby.svg?style=flat)](https://gitter.im/signac/Lobby)
[![Twitter](https://img.shields.io/twitter/follow/signacdata?style=social)](https://twitter.com/signacdata)
[![GitHub Stars](https://img.shields.io/github/stars/glotzerlab/signac?style=social)](https://github.com/glotzerlab/signac/)

The [**signac** framework](https://signac.io) helps users manage and scale file-based workflows, facilitating data reuse, sharing, and reproducibility.

It provides a simple and robust data model to create a well-defined indexable storage layout for data and metadata.
This makes it easier to operate on large data spaces, streamlines post-processing and analysis and makes data collectively accessible.

## Resources

- [Framework documentation](https://docs.signac.io/):
  Examples, tutorials, topic guides, and package Python APIs.
- [Chat Support](https://gitter.im/signac/Lobby):
  Get help and ask questions on the **signac** gitter channel.
- [**signac** website](https://signac.io/):
  Framework overview and news.

## Installation

The recommended installation method for **signac** is through **conda** or **pip**.
The software is tested for Python 3.6+ and is built for all major platforms.

To install **signac** *via* the [conda-forge](https://conda-forge.github.io/) channel, execute:

```bash
conda install -c conda-forge signac
```

To install **signac** *via* **pip**, execute:

```bash
pip install signac
```

**Detailed information about alternative installation methods can be found in the [documentation](https://docs.signac.io/en/latest/installation.html).**

## Quickstart

The framework facilitates a project-based workflow.
Set up a new project:

```bash
$ mkdir my_project
$ cd my_project
$ signac init MyProject
```

and access the project handle:

```python
>>> project = signac.get_project()
```

## Testing

You can test this package by executing:

```bash
$ python -m pytest tests/
```

## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately.
We have prepared examples on how to do that [here](http://docs.signac.io/en/latest/acknowledge.html).
**Thank you very much!**

The signac framework is a [NumFOCUS Affiliated Project](https://numfocus.org/sponsored-projects/affiliated-projects).
