# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
from setuptools import setup, find_packages


requirements = [
    # Deprecation management
    'deprecation>=2',
    # Platform-independent file locking
    'filelock~=3.0',
    # Used for version parsing and comparison
    'packaging>=15.0',
    # Progress bars
    'tqdm>=4.10.0',
]

description = "Manage large and heterogeneous data spaces on the file system."

try:
    this_path = os.path.dirname(os.path.abspath(__file__))
    fn_readme = os.path.join(this_path, 'README.md')
    with open(fn_readme) as fh:
        long_description = fh.read()
except (IOError, OSError):
    long_description = description

setup(
    name='signac',
    version='1.4.0',
    packages=find_packages(),
    zip_safe=True,
    maintainer='signac Developers',
    maintainer_email='signac-support@umich.edu',
    author='Carl Simon Adorf et al.',
    author_email='csadorf@umich.edu',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://signac.io",
    download_url="https://pypi.org/project/signac/",
    keywords='simulation database index collaboration workflow',

    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],

    install_requires=requirements,

    python_requires='>=3.5, <4',

    extras_require={
        'db': ['pymongo>=3.0'],
        'mpi': ['mpi4py'],
        'h5': ['h5py']
    },

    entry_points={
        'console_scripts': [
            'signac = signac.__main__:main',
        ],
    },
)
