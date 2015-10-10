import sys
IS_PYTHON3 = sys.version_info[0] == 3
if not IS_PYTHON3:
    print("Error: signac requires python version >= 3.x.")
    sys.exit(1)

from setuptools import setup, find_packages

setup(
    name = 'signac',
    version = '0.1.7dev1',
    packages = find_packages(),

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

    install_requires=['pymongo>=2.8', 'jsonpickle','networkx>=1.10', 'six'],

    extras_require = {
        'mpi': ['mpi4py'],
        },
    entry_points = {
        'console_scripts': [
            'signac = signac.contrib.script:main',
            'signac_init = signac.contrib.init_project:main',
            'signac_configure = signac.contrib.configure:main',
            'signac_admin = signac.admin.manage:main',
            'signac_server = signac.contrib.server:main',
            'signac_user = signac.contrib.admin:main',
            'signac_admin_project = signac.admin.manage_project:main',
        ],
    },
)
