import os
from setuptools import setup, find_packages


requirements = [
    # Deprecation management
    'deprecation>=2',
]

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
    version='1.2.0',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='simulation database index collaboration workflow',
    url="https://signac.io",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],

    install_requires=requirements,

    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',

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
