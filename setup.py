import sys
from setuptools import setup, find_packages

if sys.version_info < (2, 7, 0):
    print("Error: signac requires python version >= 2.7.x.")
    sys.exit(1)

setup(
    name='signac',
    version='0.3.0',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description="Simple data management framework.",
    keywords='simulation database index collaboration workflow',

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
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
