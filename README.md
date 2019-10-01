# easy-query

#### Makes your SQL easy to write and understand

````
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
````
