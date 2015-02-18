from setuptools import setup, find_packages

setup(
    name = 'compdb',
    version = '0.1',
    package_dir = {'': 'lib'},
    packages = find_packages('lib'),

    author = 'Carl Simon Adorf',
    author_email = 'csadorf@umich.edu',
    description = "Computational Database.",
    keywords = 'simulation tools mc md monte-carlo mongodb jobmanagement materials database',

    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: Physics",
        ],
)
