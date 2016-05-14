import sys

if sys.version_info < (2,7,0):
    print("Error: signac requires python version >= 2.7.x.")
    sys.exit(1)

from setuptools import setup, find_packages

setup(
    name='signac',
    version='0.2.8',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description="Computational Database.",
    keywords='simulation tools mc md monte-carlo mongodb '
             'jobmanagement materials database',

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: Physics",
    ],

    extras_require={
        'db': ['pymongo>=3.0'],
        'mpi': ['mpi4py'],
        'conversion': ['networkx>=1.1.0'],
        'gui': ['PySide'],
    },

    entry_points={
        'console_scripts': [
            'signac = signac.__main__:main',
            'signac-gui = signac.gui:main',
        ],
    },
)
