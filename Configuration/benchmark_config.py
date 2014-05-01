import json
import time
from AccessMode import SimpleOpt
from AccessMode import DataModel

class BenchmarkConfig:
    def __init__(self, config):
        # All ratios are out of 100
        self.rRead = config['rRead']
        self.rWrite = config['rWrite']
        self.rUpdate = config['rUpdate']

        self.nRequests = 10

        #total = sum([v for v in self.__dict__.keys() if v.startswith('r')])

        self.benchmark = []
        for i in range(self.nRequests/2):
            self.benchmark.append(SimpleOpt.put)
        for i in range(self.nRequests/2):
            self.benchmark.append(SimpleOpt.get)


    def generate_bench(self):
        pass

    def run_benchmark(self, handle):
        i = 0
        begin = time.time()
        for op in self.benchmark:
            if i%20 == 0:
                print '\b'*80, '%0.00f %%, t = %f' % (i*100.0/self.nRequests, time.time() - begin),

            item = DataModel.RecordInfo(1024, i).get_record_info()
            i = (i+1)%(self.nRequests/2)
            op(handle, item)
        dt = time.time() - begin
        print 'final = %f' % dt