'''
Created on April 17, 2014
  
@author: ChenMing
'''
import time
import uuid    
import random

from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.dynamodb2.fields import HashKey, RangeKey, AllIndex
from boto.dynamodb2.types import STRING, NUMBER

from BatchTable import BatchTable
from DataModel import RecordInfo, BaseUtil

from threading import Thread

class TableOpt(object):
    def __init__(self, conn, credentials, if_create_table):
        '''
        Constructor
        '''
        self.TABLE_NAME  = "table"
        # Max number of threads
        self.MAX_THREAD_COUNT = 40
        # Min number of tasks for each thread
        self.MIN_NUMBER_TASK_PER_THERAD = 500
        # default record size
        self.record_size = 200
        # util object
        self.util = BaseUtil()
        
        self.conn        = conn
        self.table       = None
        self.credentials = credentials
        self.table_name  =  self.credentials[self.TABLE_NAME]
        
#         boto.set_file_logger('boto', './logs/logLogFile', 'DEBUG')
        
        if (if_create_table) :
            self.create_table()
        else:
            self._get_table()
      
      
    def _scan_records(self):   
        items = self.table.scan() 
        return items
    
    
    def _query_pkg_idx(self, pkg_id, partition_idx):
        log_files = self.table.query(
                            partitionIndex__eq = partition_idx,
                            pkgId__eq = pkg_id,
                            index = 'LogPackageIdIndex')
        return log_files
      
      
    def _get_table(self):
        if(self.table_name in self.conn.list_tables()['TableNames']):
            self.table = Table(self.table_name, 
                           connection = self.conn)
        else:
            raise Exception("Do create table before other operations")
      
      
    def create_table(self):
        try:           
            self.table = Table.create(self.table_name, 
                          schema = [
                                  HashKey('PartitionID', data_type=NUMBER),
                                  RangeKey('FileName', data_type = STRING),
                          ], throughput = {
                                  'read' : 50,
                                  'write': 30,
                          },
                          connection = self.conn
                      )
            print "Create " + self.TABLE_NAME + " succeed\n"
        except Exception as e:
            print e 
            print "Create " + self.TABLE_NAME + " failed\n"
        
        
    def _create_log_file_url(self):
        # Create random unique log file url
        magic_time = int(time.time())
        rand = random.randint(1, 100000)
        log_file_url = "s3n://eagle-us-east-1-cheming-eaglembs//Eagle/eaglegen-us-east-1/EagleService/RDS-TEST/" \
                         + self.credentials["s3_bucket_name"] + \
                         str(id(magic_time)) + str(rand) + str(uuid.uuid4()) + \
                         ".gz"
        return log_file_url
    
    
    def insert_records(self, numberOfRecords, record_size):
        if not record_size:
            record_size = self.record_size
        for i in range(0, numberOfRecords):
            record = RecordInfo(record_size, i)
            item_data  = record.get_record_info()
            self.table.put_item(item_data)
        print "done"
             
             
    def get(self, range_key):
        key = self.util.get_PartitionID(range_key)
        record = self.table.get_item(PartitionID=key, FileName=range_key)
        print 'FileName:' + record['FileName'] + "\tSize:" + str(record["Size"]) 
       
    def range_get(self, key, ranges):
        # required to provide hash key and range key for range search
        records = self.table.query_2(PartitionID__eq=int(key), FileName__beginswith=ranges)
        count = 0 
        for record in records:
            print 'FileName:' + record['FileName'] + "\tSize:" + str(record["Size"])
            count += 1
        print "# Records got: " + str(count)
    
    def update(self, range_key, record_size):
        key = self.util.get_PartitionID(range_key)
        if not record_size:
            record_size = self.record_size
        new_item = RecordInfo(record_size, 0)
        record = self.table.get_item(PartitionID=key, FileName=range_key)
        record["Size"] = new_item.get_record_info()['Size']
        record["Data"] = new_item.get_record_info()['Data']
        # new_item.set_PartitionID(key)
        # new_item.set_FileName(range_key)
        # new_record = new_item.get_record_info()
        # record = Item(self.table, data=new_record)
        if record.save():
            print "Done"
   
    def scan(self, record_size):
        records = self.table.scan(Size__eq=record_size, )
        count = 0
        for record in records:
            print 'FileName:' + record['FileName'] + "\tSize:" + str(record["Size"]) 
            count += 1
        print "# Records got: " + str(count)

    def batch_insert_records(self, number_of_records, record_size):
        if record_size == 0:
            record_size = self.record_size
        with BatchTable(self.table) as batch:
            for i in range(0, number_of_records):
                record = RecordInfo(record_size, i)
                item_data  = record.get_record_info()
                batch.put_item(data = item_data)
        print "done"
    
    
    def multithread_insert(self, number_of_records):
        threads = []
        
        # Balance tasks; make sure each task at least has min number tasks except the last one
        if ((self.MIN_NUMBER_TASK_PER_THERAD * self.MAX_THREAD_COUNT) > number_of_records):
            number_of_task_per_thread = self.MIN_NUMBER_TASK_PER_THERAD
        else:
            number_of_task_per_thread = number_of_records / self.MAX_THREAD_COUNT
        
        i = 0
        while i < number_of_records:
            inserting_task = None
            if ((number_of_records - i) > self.MIN_NUMBER_TASK_PER_THERAD):
                inserting_task = Thread(target = self.batch_insert_records, args = (number_of_task_per_thread, ))
            else:
                inserting_task = Thread(target = self.batch_insert_records, args = (number_of_records - i, ))
            threads.append(inserting_task)
            inserting_task.start()
            i += number_of_task_per_thread
        
        for thread in threads:
            thread.join()
        
        print "Successed insert " + str(number_of_records) + " items"
        
        
    def list_records(self):
        files = self._scan_records()
        for record in files:
            print record['FileName']
    
    
    def count_records(self):
        count = 0
        kwargs = {}
        kwargs['select'] = 'COUNT'
        raw_results = self.table.connection.scan(
            self.table_name,
            **kwargs
        )
        count += raw_results.get('Count', int)
        last_key_seen = raw_results.get('LastEvaluatedKey', None)
        while last_key_seen:
            kwargs['exclusive_start_key'] = last_key_seen
            raw_results = self.table.connection.scan(
                self.table_name,
                **kwargs
            )
            count += raw_results.get('Count', int)
            last_key_seen = raw_results.get('LastEvaluatedKey', None)
        print "total number of records: " + str(count)
    
    
    def count_one_page(self):
        kwargs = {}
        kwargs['select'] = 'COUNT'
        raw_results = self.table.connection.scan(
            self.table.table_name,
            **kwargs
        )
        count = raw_results.get('Count', int)
        print "Number of records in on page: " + str(count)
    
    
    def delete_records(self):
        records = self._scan_records() 
        count = 0
        for record in records:
            record.delete()
            count += 1
        print "Delete " + str(count) + " items\n"
          
          
    def batch_delete_records(self):
        records = self._scan_records() 
        count   = 0  
        with BatchTable(self.table) as batch:
            for record in records:
                batch.delete_item(
                    PartitionID = record["PartitionID"],
                    FileName = record["FileName"]
                )   
                count += 1
        print "Delete " + str(count) + " items\n"
     
     
    # Used by packageDao
    def pkg_delete_helper(self, pkg):
        count = 0  
        with BatchTable(self.table) as batch:
            for idx in pkg["logFilePartitionIndexSet"]:
                log_files = self._query_pkg_idx(pkg["pkgId"], idx)
                for log in log_files:
                    batch.delete_item(
                        partitionIndex = log["partitionIndex"],
                        logFileUrl = log["logFileUrl"]
                    )   
                    count += 1
        print "Delete " + str(count) + " logs in pkg:" + pkg["pkgId"] + "\n"
        
        
    def delete_table(self):
        self.table.delete()
        
  
