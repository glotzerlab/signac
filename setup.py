import os
from setuptools import setup, find_packages

description = "Simple file data management database."

try:
    this_path = os.path.dirname(os.path.abspath(__file__))
    fn_readme = os.path.join(this_path, 'README.md')
    with open(fn_readme) as fh:
        long_description = fh.read()
except (IOError, OSError):
    long_description = description

setup(
    name='signac',
    version='0.9.2',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='simulation database index collaboration workflow',
    url="https://bitbucket.org/glotzer/signac",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],

    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',

    extras_require={
        'db': ['pymongo>=3.0'],
        'mpi': ['mpi4py'],
    },

    entry_points={
        'console_scripts': [
            'signac = signac.__main__:main',
        ],
    },
)
