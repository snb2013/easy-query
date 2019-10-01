from django.db import connection

from easy_query.query import Query
from easy_query.connectors.django import DjangoConnector

q = Query()
customers = q.table('customers')\
    .field('id')\
    .field('name')
q.table('purchases')\
    .field('id')\
    .field('date')\
    .relation('customer_id', customers)
print(q.sql)

connector = DjangoConnector(q, connection)
print(connector.to_list())
print(connector.to_dict('name'))
print(connector.to_grouped_dict('name'))

q = Query().limit(10)
customers = q.table('customers').field('id').field('name')
q.table('purchases').field('id').field('date')\
    .relation('customer_id', customers)\
    .condition('date', ['2018-01-01', '2018-12-31'], is_range=True)
print(q.sql)

connector = DjangoConnector(q, connection)
print(connector.to_list())
print(connector.to_dict('name'))
print(connector.to_grouped_dict('name'))
