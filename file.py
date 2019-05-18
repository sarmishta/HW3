import heapq
import time
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCountTopN(MRJob):
   
    def mapper(self, _, line):
        # flag=0
        # global line_cols1
        line1=""
        if(line.startswith('WARC-Target-URI')  or line.startswith('Content-Length')):
            line1=line1+line
            yield line1,""
 
    def steps(self):
        return [
            MRStep(mapper=self.mapper)
      
            ]

if __name__ == "__main__":
    start=time.time()
    WordCountTopN.run()
    end=time.time()
    sys.stderr.write(str(end-start))

