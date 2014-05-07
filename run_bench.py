from Configuration import CredentialsParser, BenchmarkConfig
from AccessMode import SimpleOpt
from optparse import OptionParser

import config
import sys

import boto.dynamodb2

def main(args):
    # with open("credential.json") as f:
    #     credentials_str = f.read()
    #     credentials = CredentialsParser.CredentialsParser(credentials_str)

    conn = boto.dynamodb2.connect_to_region(
        config.region
        # credentials.credentials["region"],
        # aws_access_key_id = credentials.credentials["access_id"],
        # aws_secret_access_key = credentials.credentials["access_key"]
    )

    handle = SimpleOpt(
        config.table_name,
        conn=conn
    )
    
    # config = {'nRequests':1024}
    config = {'nRequests':100}
    c = BenchmarkConfig(handle, **config)

    # c = BenchmarkConfig(handle)

    # use read portion, write portion as parameters
    # for read in [20, 40, 60, 80]:
    #     write = 100 - read
    #     params = {'rRead':read, 'rWrite':write, 'rUpdate':0}
    #     c.generate_benchmark(**params)
    
    #     print
    #     print "Read:", read, "Write:", write, ':'
    #     c.run_benchmark()
    #     print

    # use record size as parameter (512B, 1K, 2K, 4K, 8K)
    # for size in [512, 1024, 2048, 4096, 8192]:
    #     params = {'rSize':size}
    #     c.generate_benchmark(**params)

    #     print
    #     print "RecordSize:", size, ':'
    #     c.run_benchmark()
    #     print
    
    # vary total size (1M, 32M, 64M, 128M, 256M)
    for nRequests in [100, 200]:
    # for nRequests in [1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024]:
        params = {'nRequests':nRequests}
        c.generate_benchmark(**params)
       
        print
        print "Total Size:", nRequests * c.recordSize / 1024, 'KB:'
        c.run_benchmark_repeated_read()
        print

        


if __name__ == '__main__':
    main(sys.argv)
