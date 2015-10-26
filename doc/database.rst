========
Database
========

After :doc:`configuring <configuration>` one ore more database hosts you can access a database with the :py:func:`signac.db.get_database` function.

.. automodule:: signac.db
    :members:
    :undoc-members:
    :show-inheritance:

Use crawlers to create an index on your data, which you store in a database.

.. autoclass:: signac.contrib.crawler.BaseCrawler
    :noindex:
    :members:
    :undoc-members:

    .. automethod:: __init__
