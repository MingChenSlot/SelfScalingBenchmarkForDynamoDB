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

        self.nRequests = 100

        self.readBase = 100000
        self.writeBase = 3000

        #total = sum([v for v in self.__dict__.keys() if v.startswith('r')])

        self.benchmark = []
        for i in range(self.nRequests * self.rWrite / 100):
            self.benchmark.append(SimpleOpt.put)
        for i in range(self.nRequests * self.rRead / 100):
            self.benchmark.append(SimpleOpt.get)
        for i in range(self.nRequests * self.rUpdate / 100):
            self.benchmark.append(SimpleOpt.update)


    def set_up_benchmark(self):
        for i in range(self.nRequests):
            item = DataModel.RecordInfo(1024, self.readBase+i).get_record_info()

    def run_benchmark(self, handle):
        i = 0
        begin = time.time()
        prevOp = None
        for op in self.benchmark:
            if i%20 == 0:
                print '\b'*80, '%0.00f %%, t = %f' % (i*100.0/self.nRequests, time.time() - begin),

            item = DataModel.RecordInfo(1024, i).get_record_info()
            if op != prevOp:
                i = 0
            else:
                i+=1
            op(handle, item)
            prevOp = op
        dt = time.time() - begin
        print 'final = %f' % dt