.. _tutorial_extra:

============
Extra Topics
============

.. note::

    The final part of the tutorial covers a few special topics which may not be relevant to all users.

Advanced Indexing
=================

Custom crawlers
---------------

The :py:meth:`~.Project.index` function as well as the ``signac index`` command internally setup a :py:class:`~.contrib.SignacProjectCrawler` to crawl through the data space and create the index.
To have more control over the indexing process, we can do this explicitly:

.. code-block:: python
   
    from signac.contrib.crawler import SignacProjectCrawler
    from signac.contrib.formats import TextFile

    class IdealGasProjectCrawler(SignacProjectCrawler):
        pass
    IdealGasProjectCrawler.define('.*/V\.txt', TextFile)

    crawler = IdealGasProjectCrawler()
    for doc in crawler.crawl():
        print(doc)

We could specialize the ``IdealGasProjectCrawler`` class further, e.g., to add more metadata to the index.

Using a master crawler
----------------------

A master crawler uses other crawlers to compile a combined master index of one or more data spaces.
This allows you to expose your project data to you and everyone else who has access to the index.

To expose the project to a :py:class:`~signac.contrib.crawler.MasterCrawler` we need to create an :ref:`access module <signac-access>`.
For signac projects this is simplified by using the :py:meth:`~signac.contrib.project.Project.create_access_module` method.
Let's create the access module by adding the following commands to the ``create_index.py`` script:

.. code-block:: python

    # create_index.py
    # ...
    try:
        project.create_access_module({'.*/V\.txt': TextFile})
    except OSError:
        print("Access module already exists!")

Executing ``python create_index.py`` will now create a ``signac_access.py`` module in the project's root directory, which will look like this:

.. code-block:: bash

    #!/usr/bin/env python
    # -*- coding: utf-8 -*-
    import os

    from signac.contrib.crawler import SignacProjectCrawler
    from signac.contrib.formats import TextFile
    from signac.contrib.crawler import MasterCrawler


    class IdealGasProjectCrawler(SignacProjectCrawler):
        pass
    IdealGasProjectCrawler.define('.*/V\.txt', TextFile)


    def get_crawlers(root):
        return {'main': IdealGasProjectCrawler(os.path.join(root, 'workspace'))}


    if __name__ == '__main__':
        master_crawler = MasterCrawler('.')
        for doc in master_crawler.crawl(depth=1):
            print(doc)

The ``signac_access.py`` module defines a specific crawler for this project, which can be further specialized.

A master crawler will search for modules like this, imports them and then executes all crawlers defined in the ``get_crawlers()`` function.
By modifying the access module, you can control exactly what data is exposed to a master crawler.

.. note::

    The expression ``if __name__ == '__main__':`` is only True if the script is directly executed and not imported from another script.
    This means the commands below it have no relevance with regards to the script's function as access module.
    The commands are there to allow immediate testing.

Fetch data via index
--------------------

Data, which was indexed with a :py:class:`~signac.contrib.crawler.MasterCrawler` can be seamlessly fetched using the signac :py:func:`~signac.fetch` and :py:func:`~signac.fetch_one` functions.
Let's test this!

First we make a slight change to the ``signac_access.py`` file from the previous section:

.. code-block:: python

    # signac_access.py
    # ...

    if __name__ == '__main__':
        import json                                         # <- Add import line.
        master_crawler = MasterCrawler('.')
        for doc in master_crawler.crawl(depth=1):
            print(json.dumps(doc))                          # <- Dump index document in JSON format.

We then store the index in a file:

.. code-block:: bash

    $ python signac_access.py > index.txt

Next, we implememt a ``fetch.py`` script:

.. code-block:: python

    # fetch.py
    import json
    import signac

    with open('index.txt') as file:
        for line in file:
            doc = json.loads(line)
            file = signac.fetch_one(doc)
            if file is None:
                # Ignoring missing files.
                continue
            V = float(file.read())
            print(doc['statepoint'], V)

This scripts reads the index documents from the index file.
The index document is stored in the ``doc`` variable and contains the link to the indexed file.
We pass the ``doc`` variable to the :py:func:`~signac.fetch_one` function to open the file and then print its content to screen.

.. code-block:: bash

    $ python fetch.py
    {'p': 10.0, 'N': 1000, 'T': 1.0} 100.0
    {'p': 4.5, 'N': 1000, 'T': 1.0} 222.22222222222223
    {'p': 7.800000000000001, 'N': 1000, 'T': 1.0} 128.2051282051282
    # ...

Database Integration
--------------------

Instead of storing the index in a plain-text file we could export it to any tool of our choice.
For convenience, signac provides export routines for MongoDB database collections.

If we :ref:`configured <configuration>` a MongoDB database we could export the index to a database collection:

.. code-block:: python

    # create_index.py
    import signac

    project = signac.get_project()
    db = signac.get_database('mydb')
    signac.contrib.export_pymongo(project.index(), db.index)

Or using the master crawler:

.. code-block:: python

    # signac_acess.py
    # ...
    if __name__ == '__main__':
        master_crawler = MasterCrawler('.')
        signac.contrib.export_pymongo(crawler.crawl(depth=1), db.index)

This would allow us to execute more advanced query operations.
For example, to fetch all data for pressures greater than 2.0:

.. code-block:: python

    docs = db.index.find({'statepoint.p': {'$gt': 2.0}})
    for doc in docs:
        file = signac.fetch_one(doc)
        V = float(file.read())
        print(doc['statepoint'], V)

Integrating other tools
=======================

Many workflows require the integration of non-python tools.
Let's stick to the example and implement the ideal gas program in bash.
As bash can only evaluate expressions with integer values we need to express the pressure as a fraction and otherwise assume that *N* and *T* are integer values:

.. code-block:: bash

    # idg.sh
    N=$1
    T=$2
    p_num=$3        # bash expressions can only contain integers.
    p_denom=${4-1}  # The denominator defaults to 1.
    V=${expr $N \* $T \* $p_denom / $p_num}
    echo $V

We should now test our program on the command line:

.. code-block:: bash

   $ bash idg.sh 1000 1 1
   1000

There are many different ways on how to integrate this tool into our workflow.
One alternative would be to take advantage of signac's command line interface:

.. code-block:: bash

    $ signac job '{"N": 1000, "T": 1.0, "p": 1.0}'
    9e100da58ccdf6ad7941fce7d14deeb5

We could pipe the results of the computation into a file like this:

.. code-block:: bash

    $ bash idg.sh 1000 1 1 > `signac job -cw '{"N": 1000, "T": 1.0, "p": 1.0}'`/V.txt

Another alternative is to use a python script to prepare the execution of the other tool.
This has the additional advantage that we can use the :py:mod:`fractions` module to work-around bash's integer limitation:

.. code-block:: python

    # prepare_idg.py
    from fractions import Fraction
    import signac

    cmd = 'bash idg.sh {N} {T} {p_n} {p_d} > {out}'

    project = signac.get_project()
    for job in project.find_jobs():
        sp = job.statepoint()
        p = Fraction(sp['p'])
        print(cmd.format(
            N=int(sp['N']), T=int(sp['T']),
            p_n=p.numerator, p_d=p.denominator,
            out=job.fn('V.txt')))

This will generate a chain of one command for each state point in our data space:

.. code-block:: bash

    $ python prepare_idg.py
    bash idg.sh 1000 1 10 1 > ~/ideal_gas_project/workspace/07dc3f53615713900208803484b87253/V.txt
    bash idg.sh 1000 1 9 2 > ~/ideal_gas_project/workspace/14ba699529683f7132c863c51facc79c/V.txt
    # ...

To execute this we could simply pipe these commands into another bash script:

.. code-block:: bash

    $ python prepare_idg.py > run.sh
    $ bash run.sh
    $ # Or execute directly:
    $ python prepare_idg.py | bash


Custom Views
============

Sometimes it is advantageous to implement your own custom view routine.
This is an example for a flat linked view:

.. code-block:: python

    # create_flat_view.py
    import os
    import json

    import signac

    project = signac.get_project()
    statepoint_index = project.build_job_statepoint_index(exclude_const=True)

    for key, job_ids in dict(statepoint_index).items():
          sp = json.loads(key)
          name = '_'.join(str(x) for x in sp)
          dst = name + '_V.txt'
          os.symlink(job.fn('V.txt'), dst)

The :py:meth:`~.Project.build_job_statepoint_index` method generates a statepoint index, with complete statepoint paths as keys and a set of all corresponding jobs as value.
To create the flat view, we make sure to exclude all parameters which are constant over the whole data space by setting ``exclude_const=True``.

Executing this script, will create multiple symbolic links pointing to the source files with a parameter-based, human-readable name:

.. code-block:: bash

    $ python create_flat_view.py
    $ ls -1 *.txt
    p_0.1_V.txt
    p_10.0_V.txt
    # ...


Further reading
===============

This concludes the tutorial.
To learn more about the individual components, check out the :ref:`guide` or inspect the :ref:`api` documentation.
A quick overview of the most important components are provided in the :ref:`quickreference`.
