class Joins(object):
    inner_join = 1,
    left_join = 2,
    cross_join = 3

    @staticmethod
    def str(join):
        if join == Joins.inner_join:
            return 'INNER JOIN'
        elif join == Joins.left_join:
            return 'LEFT JOIN'
        else:
            return 'CROSS JOIN'


class Query(object):
    def __init__(self, raw=None):
        self.table_id = 1
        self.tables = {}
        self.table_list = []
        self.condition_groups = []
        self.orders = []
        self.groups = []
        self.limit = None
        self.distinct = False
        self.union = None
        self.raw = raw

    def table(self, name, alias=None, join=Joins.inner_join, index=None, key=None):
        table = Query.Table(name, 't%d' % self.table_id if alias is None else alias, join, index)
        self.tables[key or name] = table
        self.table_list.append(table)
        self.table_id += 1
        return table

    def condition_group(self):
        condition_group = []
        self.condition_groups.append(condition_group)
        return condition_group

    def order(self, table, field_name, desc=False):
        self.orders.append((table, field_name, desc))

    def limit(self, limit):
        self.limit = limit
        return self

    def group(self, table, field_name):
        self.groups.append((table, field_name))
        return self

    def distinct(self, value):
        self.distinct = value
        return self

    def sql(self):
        if self.sql:
            return self.sql
        tables = 'SELECT %s %s FROM %s' % ('DISTINCT' if self.distinct else '', self.get_fields_sql(),
                                           self.get_tables_sql())
        conditions = []

        # gather condition groups
        for condition_group in self.condition_groups:
            condition_group_str = []
            for condition in condition_group:
                condition_group_str.append(condition['table'].condition_as_str(condition))
            if len(condition_group_str) > 0:
                conditions.append('(%s)' % ' OR '.join(condition_group_str))

        # gather standalone conditions
        for table in self.table_list:
            condition = table.get_condition()
            if len(condition) > 0:
                conditions.append(condition)

        if len(conditions) > 0:
            sql = '%s WHERE %s' % (tables, ' AND '.join(conditions))
        else:
            sql = tables

        if len(self.groups) > 0:
            sql += ' GROUP BY ' + ','.join(['%s.%s' % (group[0].alias, group[1])
                                            for group in self.groups])

        if self.union:
            sql += ' UNION ' + self.union.sql()

        else:
            if len(self.orders) > 0:
                sql += ' ORDER BY ' + ','.join(['%s.%s%s' % (order[0].alias, order[1], ' DESC' if order[2] else '')
                                                for order in self.orders])

            if self.limit:
                sql += ' LIMIT ' + str(self.limit)

        return sql

    def get_fields_sql(self):
        return ','.join([table.get_fields() for table in self.table_list if len(table.fields) > 0])

    def get_tables_sql(self):
        tables_sql = []
        for table in self.table_list:
            # if not used_tables.has_key(table.alias):
            table_sql = ''
            if len(tables_sql) > 0:  # not first table? add JOIN clause
                table_sql += Joins.str(table.join)
            table_sql += ' %s as %s ' % (table.name, table.alias)
            if table.index:
                table_sql += ' USE INDEX (%s) ' % table.index
            if len(table.relations) > 0:
                table_sql += ' ON ' + table.get_relation()
            tables_sql.append(table_sql)
        return ' '.join(tables_sql)

    class Table(object):
        def __init__(self, name, alias, join, index):
            self.name = name
            self.alias = alias
            self.join = join
            self.index = index
            self.fields = {}
            self.field_list = []
            self.relations = []
            self.conditions = []

        def field(self, name, alias=None, aggregation=None, raw=False):
            field = {'name': name, 'alias': alias, 'aggregation': aggregation, 'table_alias': self.alias, 'raw': raw}
            self.fields[name if alias is None else alias] = field
            self.field_list.append(field)
            return self

        def relation(self, key, related_table, related_key=None):
            self.relations.append({'key': key, 'related_table': related_table, 'related_key': related_key})
            return self

        def condition(self, field, value, is_not=False, is_range=False, group=None):
            if value is None or isinstance(value, (list, tuple)) and len(value) == 0:  # ignore empty value
                return self
            condition = {'table': self, 'field': field, 'value': value, 'is_not': is_not, 'is_range': is_range}
            if group is None:
                self.conditions.append(condition)
            else:
                group.append(condition)
            return self

        def get_fields(self):
            fields = []
            for field in self.field_list:
                if field['raw']:  # use field name as is
                    field_value = field['name']
                elif isinstance(field['name'], Query):  # subquery
                    field_value = '(%s)' % field['name'].sql()
                else:
                    field_value = '%s.%s' % (self.alias, field['name'])

                if not field['aggregation'] is None:
                    field_value = '%s(%s)' % (field['aggregation'], field_value)

                if field['alias'] is None:
                    fields.append(field_value)
                else:
                    fields.append('%s AS %s' % (field_value, field['alias']))
            return ','.join(fields)

        def get_relation(self):
            relations = []
            for relation in self.relations:
                if relation['related_key']:
                    relations.append('%s.%s=%s.%s' % (self.alias, relation['key'], relation['related_table'].alias,
                                                      relation['related_key']))
                else:  # special case for joining to raw `related_table` value
                    relations.append('%s.%s=%s' % (self.alias, relation['key'], relation['related_table']))
            return ' AND '.join(relations)

        def get_condition(self):
            conditions = []
            # group conditions by AND
            for condition in self.conditions:
                condition_str = self.condition_as_str(condition)
                if condition_str is not None:  # add condition, use NOT if required
                    conditions.append(condition_str)
            return ' AND '.join(conditions)

        def condition_as_str(self, condition):
            field = condition['field']
            value = condition['value']
            is_not = condition['is_not']
            is_range = condition['is_range']
            result = ''
            if is_range:  # range is a set of 2 values. Example: (2014P01, 2014P13)
                if not value[0] is None:
                    if not value[1] is None:
                        result += '('
                    if isinstance(value[0], Query):  # field from subquery, joined by alias
                        result += '(%s.%s>=(%s))' % (self.alias, field, value[0].sql())
                    else:
                        result = '(%s.%s=%s)' % (self.alias, field, value[0])
                if not value[1] is None:
                    if not value[0] is None:
                        result += ' AND '
                    if isinstance(value[1], Query):  # field from subquery, joined by alias
                        result += '(%s.%s<=(%s))' % (self.alias, field, value[1].sql())
                    else:
                        result = '(%s.%s=%s)' % (self.alias, field, value[1])
                    if not value[0] is None:
                        result += ')'
            elif isinstance(value, dict):  # field from other table
                if isinstance(value['name'], Query):  # field from subquery, joined by alias
                    result = '(%s.%s=%s)' % (self.alias, field, value['alias'])
                else:
                    result = '(%s.%s=%s.%s)' % (self.alias, field, value['table_alias'], value['name'])
            elif isinstance(value, (list, tuple)):  # multiple values for one field are grouped by OR
                if len(value) > 0:
                    value_set = []
                    num_values = []
                    for v in value:
                        if v is None:
                            value_set.append('(%s.%s IS NULL)' % (self.alias, field))
                        else:
                            if len(num_values) == 0:
                                value_set.append('(%s.%s IN ([VALUES]))' % (self.alias, field))
                            num_values.append(str(v))
                    result = '(%s)' % ' OR '.join(value_set).replace('[VALUES]', ','.join(num_values))
            else:
                result = '(%s.%s=%s)' % (self.alias, field, value)
            return (is_not and ' NOT ' or '') + result
