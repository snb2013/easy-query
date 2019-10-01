from easy_query.connectors.base import BaseConnector


class DjangoConnector(BaseConnector):
    def get_data(self):
        if not self.query.sql or not self.query.table_list:
            return [], []

        cursor = self.connection.cursor()
        sql = self.query.sql()
        cursor.execute(sql)
        names = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return names, rows
