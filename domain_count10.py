from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import sys

class WordCountTopN(MRJob):
    def mapper(self, _, line):
        if(line.startswith('WARC-Target-URI')):
            line_cols = line.split('/')
            line_cols1 = line_cols[2].split('.')
            yield line_cols1[-1], 1
    # def reducer(self, word, counts):
    #     yield word, sum(counts)
    def reducer(self, key, values): 
       yield None, (sum(values),key)

    def secondreducer(self, key, values):   
       self.aList= []    
       for v in values:
            self.aList.append(v)
       count = len(self.aList)
       self.aList.sort(reverse=True)
       for m in range(10):
          yield self.aList[m]

    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer = self.secondreducer)
                ]

if __name__ == "__main__":
    start=time.time()
    WordCountTopN.run()
    end=time.time()
    sys.stderr.write(str(end-start))
