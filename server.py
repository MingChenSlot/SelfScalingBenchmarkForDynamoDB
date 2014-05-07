from Configuration import CredentialsParser, BenchmarkConfig
from AccessMode import SimpleOpt
from optparse import OptionParser

import config
import sys
import pickle

import boto.dynamodb2
import socket, time
import errno

# get all running hosts
def get_hosts(state='running'):
    connection = boto.ec2.connect_to_region(config.region)

    role = config.role 
    hosts = []
    for reservation in connection.get_all_instances():
        for instance in reservation.instances:
            if instance.state == state:
                for key in ['role']:
                    value = instance.tags.get(key)
                    if value and role in value.split(','):
                        hosts.append(instance.public_dns_name)
                        break
    return hosts

def main(argv):
    
    conn = boto.dynamodb2.connect_to_region(
        config.region
    )

    handle = SimpleOpt(
        table=config.table_name,
        conn=conn
    )
    
    if len(argv) != 3:
        print 'usage: python program.py workload number_instances'
        return
    workload = int(argv[1])
    number_of_instances = int(argv[2])
    hosts = get_hosts()
    # print hosts
    # hosts = ['127.0.0.1', ]
    
    params = {'nRequests':(workload/(len(hosts))), 'setup':0}
    c = BenchmarkConfig(handle, **params)

    msg = {'cmd':'start', 'workload':(workload / len(hosts))}
    writeBase = workload
    waits = []

    begin = time.time()

    for idx, host in enumerate(hosts):
        # start as many clients as we want
        if idx >= number_of_instances:
            break
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, config.port))
            # assign different writeBase to different client
            msg['writeBase'] = idx * msg['workload']
            s.send(pickle.dumps(msg))
            waits.append(s)
        except socket.error, message:
            print 'start ', host, ' failed with ', message

    for s in waits:
        feedback = s.recv(config.receive_up)  
        if not feedback.startswith('done'):
            print 'Error: ', feedback

    dt = time.time() - begin
    print 'final = %f' % dt
    print 'throughput = %f KB/s' % ((1024 * workload) / 1024 / dt)
    
    msg = {}
    msg = {'cmd':'end', }
    for s in waits:
        try:
            s.send(pickle.dumps(msg))
            s.close()
        except socket.error, message:
            print 'end ', host, ' failed with ', message


if __name__ == '__main__':
    main(sys.argv)
