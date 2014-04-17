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
    
   
class LogFileInfo(BaseUtil):
    def __init__(self, log_file_url):
        '''
        Constructor
        dummy data for log file
        '''
        PARTITION_COUNT = 13
        self.log_file = {}
        self.log_file["partitionIndex"] = (self.java_string_hashcode(log_file_url) & 0x7FFFFFFF) % PARTITION_COUNT
        self.log_file["logFileUrl"] = log_file_url
        self.log_file["version"] = 1
        self.log_file["logFileSize"] = 1024
        self.log_file["lastModifiedTimestamp"] = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        self.log_file["pkgId"] = "N/A"
        self.log_file["isFailed"] = 0
        self.log_file["retryCount"] = 0
    
    def get_log_file_info(self):
        return self.log_file
    
    def set_pkg_id(self, pkgId):
        self.log_file["pkgId"] = pkgId
          
    def get_pkg_id(self):
        return self.log_file["pkgId"]


class PackageInfo(BaseUtil):
      
    def __init__(self, pkg_id):
        '''
        Constructor
        dummy data for package
        '''
        PARTITION_COUNT = 2
        self.pkg = {}
        self.pkg["partitionIndex"] = (self.java_string_hashcode(pkg_id) & 0x7FFFFFFF) % PARTITION_COUNT
        self.pkg["pkgId"] = pkg_id
        self.pkg["version"] = 1
        self.pkg["status"] = "PACKAGE_PROCESSED_COMPLETELY"
        self.pkg["pkgGenTimestamp"] = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        self.pkg["statusTimestamp"] = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        self.pkg["logFilePartitionIndexSet"] = None
        self.pkg["type"] = "REGULAR_PKG"
        self.pkg["priority"] = "HIGH"
        self.pkg["logCount"] = 500
        self.pkg["jobFlowId"] = "j-18U5HIRTHPXZ1"
        self.pkg["hadoopJarStepConfig"] = " "
        
        self.pkg_status = ("PACKAGING_IN_PROGRESS", "PACKAGING_COMPLETE", \
                                "PACKAGE_PROCESSING", "PACKAGE_PROCESSED_COMPLETELY", \
                                "PACKAGE_PROCESSED_PARTIALLY", \
                                "PACKAGE_PROCESSING_FAILED") 
        
    def get_pkg_info(self):
        return self.pkg
    
    def set_log_file_partition_idx(self, log_partitionidx_set):
        if isinstance(log_partitionidx_set, (set, frozenset)):
            self.pkg["logFilePartitionIndexSet"] = log_partitionidx_set
        else:
            raise AttributeError
    
    def set_pkg_status(self, status):
        if status == None:
            return
        if status in self.pkg_status:
            self.pkg["status"] = status
            self.pkg["statusTimestamp"] = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        else:
            print "Illegal input status"
            raise AttributeError
        
    def get_log_count(self):
        return self.pkg["logCount"]
    
    def set_log_count(self, log_count):
        if log_count == None:
            return
        self.pkg["logCount"] = int(log_count)
        

class AccountInfo(BaseUtil):
    def __init__(self, account_id):
        '''
        Constructor
        dummy data for account
        '''
        PARTITION_COUNT = 2
        self.account = {}
        self.account["partitionIndex"] = (self.java_string_hashcode(account_id) & 0x7FFFFFFF) % PARTITION_COUNT
        self.account["awsAccountId"] = account_id
        self.account["version"] = 1
        self.account["subscriptionTimestamp"] = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
        self.account["isSubscribed"] = True
        self.account["snsTopicName"] = "eagle-cli-test-sns-topic"
        self.account["s3BucketName"] = "eagle-cli-test-bucket-name"
        self.account["s3FolderPrefix"] = str(account_id)[-3:]
        
    def get_account_info(self):
        return self.account
    
    def set_subscription_state(self, is_subscribe):
        if isinstance(is_subscribe, bool):
            self.account["isSubscribed"] = is_subscribe
        else:
            raise AttributeError
    
    def get_subscription_state(self):
        return self.account["isSubscribed"]
    
    def set_subscription_time(self, subscription_time):
        self.account["subscriptionTimestamp"] = subscription_time
        
    def get_subscription_time(self):
        return self.account["subscriptionTimestamp"]
    
    