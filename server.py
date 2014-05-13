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
                # hosts.append(instance.private_ip_address)
                # hosts.append(instance.public_dns_name)
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
    
    workload = int(argv[2])
    number_of_instances = int(argv[3])
    hosts = get_hosts()
    # hosts = ['127.0.0.1', ]
    # hosts = [config.master, ]
    print hosts
    
    params = {'nRequests':(workload/number_of_instances)}
    c = BenchmarkConfig(handle, **params)

    msg = {'cmd':'start', 'workload':(workload / number_of_instances)}
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
        s.close()

    dt = time.time() - begin
    print 'final = %f' % dt
    print 'throughput = %f KB/s' % ((2048 * workload) / 1024 / dt)
    

def end_server():
    hosts = get_hosts()
    # hosts = ['127.0.0.1', ]
    msg = {'cmd':'end', }
    for host in hosts:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, config.port))
            s.send(pickle.dumps(msg))
            s.close()
        except socket.error, message:
            print 'end ', host, ' failed with ', message
    print 'End all clients'

if __name__ == '__main__':
    if sys.argv[1] == '--end':
        end_server()
    elif sys.argv[1] == '--start':
        if len(sys.argv) != 4:
            print 'usage: python program.py [--start --end] workload number_instances'
            exit(0)
        main(sys.argv)
