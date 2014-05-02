from boto.dynamodb.types import dynamize_value
from boto.dynamodb2.table import BatchTable
from boto.dynamodb2.table import Table
import os

class SimpleOpt:
    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

        self.devnull = open(os.devnull, 'w')

    def encode(self, item):
        for key, value in item.items():
            item[key] = dynamize_value(value)
        return item

    def encode_attribs(self, attribs):
        for key, value in attribs.items():
            attribs[key] = {'Value': dynamize_value(value)}
        return attribs

    def batch_put(self, items):
        with BatchTable(Table(self.table, connection = self.conn)) as batch:
            for item in items:
                batch.put_item(data=item)

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
        keys = self.encode({'PartitionID':keys['PartitionID'], 'FileName':keys['FileName']})

        item = self.conn.get_item(
            table_name=self.table,
            key=keys,
            attributes_to_get=None,
            consistent_read=None)
        
        print >>self.devnull, item['Item']['FileName'],


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
