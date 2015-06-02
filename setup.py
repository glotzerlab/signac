from setuptools import setup, find_packages

setup(
    name = 'compdb',
    version = '0.1',
    package_dir = {'': 'src'},
    packages = find_packages('src'),

    author = 'Carl Simon Adorf',
    author_email = 'csadorf@umich.edu',
    description = "Computational Database.",
    keywords = 'simulation tools mc md monte-carlo mongodb jobmanagement materials database',

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: Physics",
        ],

    install_requires=['pymongo', 'jsonpickle','networkx'],

    entry_points = {
        'console_scripts': [
            'compdb = compdb.contrib.script:main',
            'compdb_init = compdb.contrib.init_project:main',
            'compdb_configure = compdb.contrib.configure:main',
            'compdb_admin = compdb.admin.manage:main',
            'compdb_server = compdb.contrib.server:main',
        ],
    },
)
