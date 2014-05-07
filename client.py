from Configuration import CredentialsParser, BenchmarkConfig
from AccessMode import SimpleOpt
from optparse import OptionParser

import config
import sys
import pickle
import socket

import boto.dynamodb2

def main(argv):
    
    conn = boto.dynamodb2.connect_to_region(
        config.region
    )

    handle = SimpleOpt(
        table=config.table_name,
        conn=conn
    )

    params = {'setup':0}
    c = BenchmarkConfig(handle, **params)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', config.port))
    s.listen(config.port)

    while True:
        connect, addr = s.accept()
        print "receive request from %s" % str(addr)
        raw_msg = connect.recv(config.receive_up)
        print pickle.loads(raw_msg)
        msg = pickle.loads(raw_msg) # receive up to 2K bytes
        if 'cmd' not in msg:
            connect.send('unknown information')
        elif msg['cmd'] == 'start' and 'workload' in msg and 'writeBase' in msg:
            params = {'nRequests':msg['workload']}
            c.generate_benchmark(**params)
            c.run_benchmark()
            connect.send('done')
        elif msg['cmd'] == 'end':
            break
        else:
            connect.send('insufficient information')


if __name__ == '__main__':
    main(sys.argv)
