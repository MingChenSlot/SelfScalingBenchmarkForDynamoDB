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


