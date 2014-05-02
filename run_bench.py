from Configuration import CredentialsParser, BenchmarkConfig
from AccessMode import SimpleOpt
from optparse import OptionParser

import sys

import AccessMode.boto.dynamodb2

def main(args):
    with open("credential.json") as f:
        credentials_str = f.read()
        credentials = CredentialsParser.CredentialsParser(credentials_str)

    conn = AccessMode.boto.dynamodb2.connect_to_region(
        credentials.credentials["region"],
        aws_access_key_id = credentials.credentials["access_id"],
        aws_secret_access_key = credentials.credentials["access_key"]
    )

    handle = SimpleOpt(
        table='SelfScalingBenchTest',
        conn=conn
    )
    
    c = BenchmarkConfig(handle)

    # use read portion, write portion as parameters
    # for read in [20, 40, 60, 80]:
    #     write = 100 - read
    #     params = {'rRead':read, 'rWrite':write, 'rUpdate':0}
    #     c.generate_benchmark(**params)
    
    #     print
    #     print "Read:", read, "Write:", write, ':'
    #     c.run_benchmark()
    #     print

    # use record size as parameter
    # for size in [512, 1024, 2048, 4096, 8192]:
    #     params = {'rSize':size}
    #     c.generate_benchmark(**params)
        
    #     print
    #     print "RecordSize:", size, ':'
    #     c.run_benchmark()
    #     print
    
    # vary total size (100K, 1M, 64M, 256M, 512M)
    for nRequests in [100, 1024, 64 * 1024, 256 * 1024, 512 * 1024]:
    # for nRequests in [100, 200]:
        params = {'nRequests':nRequests}
        c = BenchmarkConfig(handle, **params)
        c.generate_benchmark()
       
        print
        print "Total Size:", nRequests * c.recordSize, ':'
        c.run_benchmark()
        print

        


if __name__ == '__main__':
    main(sys.argv)
