=======
Drivers
=======

With grizly drivers you can build complex queries upon objects in sources.
QFrame factory function returns right driver based on provided dsn.

>>> from grizly import QFrame
>>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
>>> print(qf)
SELECT "customer_id",
      "sales"
FROM grizly.sales

.. toctree::
   drivers/sql
   drivers/sfdc





