from boto.dynamodb.types import dynamize_value

class SimpleOpt:
    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

    def encode(self, item):
        for key, value in item.items():
            item[key] = dynamize_value(value)
        return item

    def encode_attribs(self, attribs):
        for key, value in attribs.items():
            attribs[key] = {'Value': dynamize_value(value)}
        return attribs

    def put(self, item):
        """
        @param item a dict object representing attributes; must contain the primary key
        """
        item = self.encode(item)

        self.conn.put_item(
            table_name=self.table,
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

    def update(self, keys, values):
        """
        @param keys a dict object representing attributes; must contain the primary key
        @param values a dict object representing attributes that need to be changed
        """
        keys = self.encode(keys)
        values = self.encode_attribs(values)

        self.conn.update_item(
             table_name=self.table,
             key=keys,
             attribute_updates=values,
             expected=None)