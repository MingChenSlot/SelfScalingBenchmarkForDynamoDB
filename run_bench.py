from Configuration import CredentialsParser
from AccessMode import SimpleOpt
from optparse import OptionParser

import AccessMode.boto.dynamodb2

def main():
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

    keys = {'PartitionID': 1001, 'FileName': 'myfile'}
    # handle.put(keys)
    handle.update(keys, {'Data':'RW', 'Size':'12'})

if __name__ == '__main__':
    main()