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

    rRead, rWrite, rUpdate = 50, 50, 0
    for read in [20, 40, 60, 80]:
        write = 100-write
        config = {'rRead':read, 'rWrite':write, 'rUpdate':0}
        c = BenchmarkConfig(config)

        print
        print read, write, ':'
        c.run_benchmark(handle)
        print


if __name__ == '__main__':
    main(sys.argv)