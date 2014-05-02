import json
import time
from AccessMode import SimpleOpt
from AccessMode import DataModel

class BenchmarkConfig:
    def __init__(self, handle, nRequests=100):
        self.recordSize = 1024 # default record size
        self.nRequests = nRequests

        self.readBase = 0
        self.writeBase = self.nRequests
        self.handle = handle

        self.readSet = [SimpleOpt.get, SimpleOpt.update]
        self.writeSet = [SimpleOpt.put]
        #total = sum([v for v in self.__dict__.keys() if v.startswith('r')])

        # set up the read set
        self.__set_up_benchmark()

    def __set_up_benchmark(self):
        items = []
        for i in range(self.nRequests):
            item = DataModel.RecordInfo(self.recordSize, self.readBase+i).get_record_info()
            items.append(item)
        SimpleOpt.batch_put(self.handle, items)


    def generate_benchmark(self, rRead=80, rWrite=20, rUpdate=0, rSize=1024):
        # All ratios are out of 100
        self.rRead = rRead
        self.rWrite = rWrite
        self.rUpdate = rUpdate

        self.writeBase += self.nRequests

        self.benchmark = []
        for i in range(self.nRequests * self.rWrite / 100):
            self.benchmark.append(SimpleOpt.put)
        for i in range(self.nRequests * self.rRead / 100):
            self.benchmark.append(SimpleOpt.get)
        for i in range(self.nRequests * self.rUpdate / 100):
            self.benchmark.append(SimpleOpt.update)

        if rSize == self.recordSize:
            return
        # If iterating record size, update the record size here
        self.recordSize = rSize
        for i in range(self.nRequests):
            item = DataModel.RecordInfo(self.recordSize, self.readBase+i).get_record_info()
            SimpleOpt.update(self.handle,
                    {'PartitionID':item['PartitionID'], 'FileName':item['FileName']},
                    {'Data':item['Data'], 'Size':item['Size']})


    def run_benchmark(self):
        i = 0
        begin = time.time()
        for op in self.benchmark:
            if i%20 == 0:
                print '\b'*80, '%0.00f %%, t = %f' % (i*100.0/self.nRequests, time.time() - begin),

            if op in self.readSet:
                item = DataModel.RecordInfo(self.recordSize, i + self.readBase).get_record_info()
            elif op in self.writeSet:
                item = DataModel.RecordInfo(self.recordSize, i + self.writeBase).get_record_info()
            else:
               raise Exception("unrecognizable operation")

            op(self.handle, item)
            i += 1

        dt = time.time() - begin
        print 'final = %f' % dt
        print 'throughput = %f' % ((self.recordSize * self.nRequests) / 1024 / dt)


