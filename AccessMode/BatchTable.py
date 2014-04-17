'''
Created on Aug 1, 2013

@author: cheming
'''
from boto.dynamodb2.items import Item

class BatchTable(object):
    """
    Used by ``Table`` as the context manager for batch writes.
    """
    def __init__(self, table):
        self.table = table
        self._to_put = []
        self._to_delete = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self._to_put and not self._to_delete:
            return False

        # Flush anything that's left.
        while (len(self._to_put) > 0 or len(self._to_delete) > 0):
            self.flush()
        return True

    def put_item(self, data, overwrite=False):
        self._to_put.append(data)

        if self.should_flush():
            self.flush()

    def delete_item(self, **kwargs):
        self._to_delete.append(kwargs)

        if self.should_flush():
            self.flush()

    def should_flush(self):
        if len(self._to_put) + len(self._to_delete) == 25:
            return True

        return False

    def flush(self):
        batch_data = {
            self.table.table_name: [
                # We'll insert data here shortly.
            ],
        }

        for put in self._to_put:
            item = Item(self.table, data=put)
            batch_data[self.table.table_name].append({
                'PutRequest': {
                    'Item': item.prepare_full(),
                }
            })

        for delete in self._to_delete:
            batch_data[self.table.table_name].append({
                'DeleteRequest': {
                    'Key': self.table._encode_keys(delete),
                }
            })
            
        
        response = self.table.connection.batch_write_item(batch_data)
        self._to_put = []
        self._to_delete = []
        
        # handle unprocessed items
        unprocessed = response.get('UnprocessedItems', None)
        if unprocessed:
            unprocessed_list = unprocessed[self.table.table_name]
            item = Item(self)
            for u in unprocessed_list:
                if u.has_key("PutRequest"):
                    item.load({
                        'Item': u['PutRequest']['Item'],
                    })
                    self._to_put.append(item._data)
                elif u.has_key("DeleteRequest"):
                    item.load({
                        'Item': u['DeleteRequest']['Key'],
                    })
                    self._to_delete.append(item._data)
                else:
                    raise Exception("Error respond")
        return True
    
    