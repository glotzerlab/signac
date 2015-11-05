import sys
IS_PYTHON3 = sys.version_info[0] == 3
if not IS_PYTHON3:
    print("Error: signac requires python version >= 3.x.")
    sys.exit(1)

from setuptools import setup, find_packages

setup(
    name='signac',
    version='0.2.0',
    packages=find_packages(),

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

    install_requires=['six'],

    extras_require={
        'db': ['pymongo>=3.0'],
        'mpi': ['mpi4py'],
        'conversion': ['networkx>=1.1.0'],
    },
)
