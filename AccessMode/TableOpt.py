'''
Created on April 17, 2014
  
@author: ChenMing
'''
import time
import uuid    
import random
import boto

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, AllIndex
from boto.dynamodb2.types import STRING, NUMBER

from BatchTable import BatchTable
from DataModel import RecordInfo

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
        self.record_size = 200
        
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
    
    
    def insert_records(self, numberOfRecords):
        for i in range(0, numberOfRecords):
            record = RecordInfo(self.record_size)
            item_data  = record.get_record_info()
            self.table.put_item(item_data)
             
             
    # Used by packageDao, return log file index set; non-batch version
    def pkg_insert_helper(self, number_of_records, pkg_id):
        log_file_index_set = set()
        for i in range(0, number_of_records):
            log_file_info = LogFileInfo(self._create_log_file_url())
            log_file_info.set_pkg_id(pkg_id)
            item_data  = log_file_info.get_log_file_info()
            self.table.put_item(data = item_data)
            log_file_index_set.add(item_data["partitionIndex"])
        return log_file_index_set
    
    
    def batch_insert_records(self, number_of_records):
        with BatchTable(self.table) as batch:
            for i in range(0, number_of_records):
                log_file_info = LogFileInfo(self._create_log_file_url())
                item_data  = log_file_info.get_log_file_info()
                batch.put_item(data = item_data)
    
    
    #  Used by packageDao, return log file index set; batch version
    def pkg_batch_insert_helper(self, number_of_records, pkg_id):
        log_file_index_set = set()
        with BatchTable(self.table) as batch:
            for i in range(0, number_of_records):
                log_file_info = LogFileInfo(self._create_log_file_url())
                log_file_info.set_pkg_id(pkg_id)
                item_data  = log_file_info.get_log_file_info()
                batch.put_item(data = item_data)
                log_file_index_set.add(item_data["partitionIndex"])
        return log_file_index_set
    
    
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
        log_files = self._scan_records() 
        count = 0
        for log in log_files:
            log.delete()
            count += 1
        print "Delete " + str(count) + " items\n"
          
          
    def batch_delete_records(self):
        log_files = self._scan_records() 
        count     = 0  
        with BatchTable(self.table) as batch:
            for log in log_files:
                batch.delete_item(
                    partitionIndex = log["partitionIndex"],
                    logFileUrl = log["logFileUrl"]
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
        
  
