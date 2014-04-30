from boto.dynamodb.types import dynamize_value

class SimpleOpt:
    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

    def encode(self, item):
        for key, value in item.items():
            item[key] = dynamize_value(value)
        return item

    def put(self, item):
        """
        @param item a dict object representing attributes; must contain the primary key
        """
        item = self.encode(item)

        self.conn.put_item(
            table_name=table,
            item=item,
            expected=None)

    def get(self, keys):
        """
        @param keys a dict object representing attributes; must contain the primary key
        """
        keys = self.encode(keys)

        item = self.conn.get_item(
            table_name=self.table,
            key=keys,
            attributes_to_get=None,
            consistent_read=None)

        print item