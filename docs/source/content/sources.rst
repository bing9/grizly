=======
Sources
=======

Grizly Source factory function helps easily view and manage objects in different data sources.
After configuring a source you can easily connect to it using datasource name (dsn).

>>> from grizly import Source
>>> source = Source(dsn="redshift_acoe")
>>> source.get_tables(schema="grizly")
[('grizly', 'track'), ('grizly', 'table_tutorial'), ('grizly', 'sales')]

.. toctree::
   sources/redshift
   sources/aurora
   sources/denodo
   sources/sfdc
   sources/tableau
   sources/sqlite
   sources/mariadb