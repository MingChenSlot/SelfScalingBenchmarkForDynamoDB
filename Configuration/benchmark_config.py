import json
import time
from AccessMode import SimpleOpt
from AccessMode import DataModel

class BenchmarkConfig:
    def __init__(self, handle):
        self.nRequests = 10

        self.readBase = 0
        self.writeBase = self.nRequests
        self.handle = handle 

        self.readSet = [SimpleOpt.get, SimpleOpt.update]
        self.writeSet = [SimpleOpt.put]
        #total = sum([v for v in self.__dict__.keys() if v.startswith('r')])
        
        # set up the read set
        self.__set_up_benchmark()

    def __set_up_benchmark(self):
        for i in range(self.nRequests):
            item = DataModel.RecordInfo(1024, self.readBase+i).get_record_info()
            SimpleOpt.put(self.handle, item)

    def generate_benchmark(self, config):
        # All ratios are out of 100
        self.rRead = config['rRead']
        self.rWrite = config['rWrite']
        self.rUpdate = config['rUpdate']

        self.writeBase += self.nRequests 

        self.benchmark = []
        for i in range(self.nRequests * self.rWrite / 100):
            self.benchmark.append(SimpleOpt.put)
        for i in range(self.nRequests * self.rRead / 100):
            self.benchmark.append(SimpleOpt.get)
        for i in range(self.nRequests * self.rUpdate / 100):
            self.benchmark.append(SimpleOpt.update)


    def run_benchmark(self):
        i = 0
        begin = time.time()
        for op in self.benchmark:
            if i%20 == 0:
                print '\b'*80, '%0.00f %%, t = %f' % (i*100.0/self.nRequests, time.time() - begin),

            if op in self.readSet:
                item = DataModel.RecordInfo(1024, i + self.readBase).get_record_info()
            elif op in self.writeSet:
                item = DataModel.RecordInfo(1024, i + self.writeBase).get_record_info()
            else:
               raise Exception("unrecognizable operation")
            
            op(self.handle, item)
            i += 1
        
        dt = time.time() - begin
        print 'final = %f' % dt

     
