'''
Created on Jul 20, 2013

@author: ChenMing
'''
import json

class CredentialsParser(object):
    '''
    classdocs
    '''

    def __init__(self, credentials):
        '''
        Constructor
        '''
        self.credentials = {}
        self.credentials = json.loads(credentials)
        
    
    def printCredentials(self):
        print self.credentials