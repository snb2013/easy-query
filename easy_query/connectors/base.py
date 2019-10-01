class BaseConnector(object):
    def __init__(self, query, connection):
        self.query = query
        self.connection = connection

    def get_data(self):
        raise NotImplemented()

    def to_indexed_list(self, field_name, convert_index_to_str=False):
        names, rows = self.get_data()
        if isinstance(field_name, (list, tuple)):
            field_indexes = [names.index(name) for name in field_name]
        else:
            field_indexes = [names.index(field_name)]
        if convert_index_to_str:
            result = list(('_'.join([unicode(row[index]).lower() for index in field_indexes]), dict(zip(names, row)))
                          for row in rows)
        else:
            result = list((tuple([row[index] for index in field_indexes]), dict(zip(names, row))) for row in rows)
        return result

    def to_list(self):
        names, rows = self.get_data()
        result = list(dict(zip(names, row)) for row in rows)
        return result

    def to_dict(self, field_name, convert_index_to_str=False):
        """
        dictionary of data indexed by field_name
        field_name can be list or tuple, it is useful for multi index
        """
        result = dict(self.to_indexed_list(field_name, convert_index_to_str))
        return result

    def to_grouped_dict(self, field_name, convert_index_to_str=False):
        """dictionary of data indexed by field_name and grouped by index"""
        data = self.to_indexed_list(field_name, convert_index_to_str)
        result = {}
        # group values by requested index
        for value in data:
            key = value[0]
            if key not in result:
                result[key] = []
            result[key].append(value[1])

        return result
