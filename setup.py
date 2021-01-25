# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os

from setuptools import find_packages, setup

requirements = [
    # Deprecation management
    "deprecation>=2",
    # Platform-independent file locking
    "filelock~=3.0",
    # Used for version parsing and comparison
    "packaging>=15.0",
    # Progress bars
    "tqdm>=4.10.0",
]

description = "Manage large and heterogeneous data spaces on the file system."

try:
    this_path = os.path.dirname(os.path.abspath(__file__))
    fn_readme = os.path.join(this_path, "README.md")
    with open(fn_readme) as fh:
        long_description = fh.read()
except OSError:
    long_description = description

setup(
    name="signac",
    version="1.6.0",
    packages=find_packages(),
    zip_safe=True,
    maintainer="signac Developers",
    maintainer_email="signac-support@umich.edu",
    author="Carl Simon Adorf et al.",
    author_email="csadorf@umich.edu",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://signac.io",
    download_url="https://pypi.org/project/signac/",
    keywords="simulation database index collaboration workflow",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    project_urls={
        "Homepage": "https://signac.io",
        "Documentation": "https://docs.signac.io",
        "Source Code": "https://github.com/glotzerlab/signac",
        "Issue Tracker": "https://github.com/glotzerlab/signac/issues",
    },
    install_requires=requirements,
    # Supported versions are determined according to NEP 29.
    # https://numpy.org/neps/nep-0029-deprecation_policy.html
    python_requires=">=3.6, <4",
    extras_require={"db": ["pymongo>=3.0"], "mpi": ["mpi4py"], "h5": ["h5py"]},
    entry_points={"console_scripts": ["signac = signac.__main__:main"]},
)
