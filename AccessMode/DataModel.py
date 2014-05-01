'''
Created on Aug 2, 2013

@author: cheming
'''
from time import gmtime, strftime

class BaseUtil():
    def __init__(self):
        self.PARTITION_COUNT = 13
        pass
    
    def convert_n_bytes(self, n, b):
        bits = b*8
        return (n + 2**(bits-1)) % 2**bits - 2**(bits-1)
    
    def convert_4_bytes(self, n):
        return self.convert_n_bytes(n, 4)
    
    #Java hashCode function implementation
    def java_string_hashcode(self, s):
        h=0
        n = len(s)
        for i, c in enumerate(s):
            h = h + ord(c)*31**(n-1-i)
        return self.convert_4_bytes(h)
    
    def get_PartitionID(self, FileName):
        return (self.java_string_hashcode(FileName) & 0x7FFFFFFF) % self.PARTITION_COUNT
   
class RecordInfo(BaseUtil):
    def __init__(self, size, iteration):
        '''
        Constructor
        dummy data for our test table
        '''
        ts = strftime("%Y-%m-%dT%H:%M:%S", gmtime())
        self.PARTITION_COUNT = 13
        self.record = {}
        self.record["Data"] = "0"
        self.record["FileName"] = "File_%020d" % iteration
        self.record["PartitionID"] = iteration
        if size < 55:
            self.record["Size"] = 55
        else:
            self.record["Size"] = size
        for i in range(0, size - 55):
            self.record["Data"] = self.record["Data"] + "1";
    
    def get_record_info(self):
        return self.record
    
    def set_PartitionID(self, ID):
        self.record["PartitionID"] = ID

    def set_FileName(self, FileName):
        self.record["FileName"] = FileName

    def get_key_record_items(self):
        t = {}
        t['PartitionID'] = self.record['PartitionID']
        t['FileName'] = self.record['FileName']
        return t

    def _set_PartitionID(self):
        if "FileName" not in self.record:
            raise Exception("Do not call private function!")
        self.record["PartitionID"] = self.get_PartitionID(self.record["FileName"])

