#!/usr/bin/env python
#coding=utf-8
import boto.ec2

region = 'us-west-2'

table_name = 'SelfScalingBenchTest'

# socket port
port = 12345
# max char for receiving is 2048 Bytes
receive_up = 2048

# instance role
role = 'testcluster'

master = "ec2-54-187-114-133.us-west-2.compute.amazonaws.com"

def get_hosts(state='running'):
    connection = boto.ec2.connect_to_region(config.region)

    role = role 
    hosts = []
    for reservation in connection.get_all_instances():
        for instance in reservation.instances:
            if instance.state == state:
                for key in role_synonyms:
                    value = instance.tags.get(key)
                    if value and role in value.split(','):
                        hosts.append(instance.public_dns_name)
                        break
    return hosts

