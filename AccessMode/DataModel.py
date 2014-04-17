'''
Created on Aug 2, 2013

@author: cheming
'''
from time import gmtime, strftime

class BaseUtil():

    def __init__(self):
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
    
   
class RecordInfo(BaseUtil):
    def __init__(self, size):
        '''
        Constructor
        dummy data for our test table
        '''
        ts = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        PARTITION_COUNT = 13
        self.record = {}
        self.record['Data'] = "0"
        self.record["PartitionID"] = (self.java_string_hashcode(ts) & 0x7FFFFFFF) % PARTITION_COUNT
        self.record["FileName"] = "TestFile_" + ts
        self.record["version"] = 1
        self.record["Size"] = size
        self.record["LastModifiedTimestamp"] = ts
        for i in range(0, size - 50):
            self.record["Data"] = self.record["Data"] + "1";
    
    def get_record_info(self):
        return self.record
    


