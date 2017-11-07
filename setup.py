import sys
from setuptools import setup, find_packages

if sys.version_info < (2, 7, 0):
    print("Error: signac requires python version >= 2.7.x.")
    sys.exit(1)

setup(
    name='signac',
    version='0.9.1',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description="Simple data management framework.",
    keywords='simulation database index collaboration workflow',
    url="https://bitbucket.org/glotzer/signac",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],

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
