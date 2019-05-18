import time
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
class WordCountTopN(MRJob):
   
    def mapper(self, _, line):
        if(line.startswith('WARC-Target-URI')):
            line_sp=line.split(" ")
            line_sp2=line_sp[1].split("/")
            line_sp3=line_sp2[2].split(".")
            yield line_sp3[-1],int(line_sp[3])
            # yield line_sp3[-1],1
    # def combiner(self,key,values):
    #     yield key,1
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
    def thirdred(self,_,values):
        d1={"com":203746,"ru":24104, "org":25329,"net":17796,"de":17638,"uk":11234,"jp":8808,"fr":7484,"ua":4056,"pl":6488}
        d2={"com":627146600,"ru":111960611, "org":63810604,"net":56577104,"de":38165361,"uk":27215961,"jp":24339751,"fr":19701359,"ua":17734371,"pl":17063385}
        d3 = dict((k, int(d2[k]) / d1[k]) for k in d2)
        print(d3)
    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer = self.secondreducer),
                MRStep(reducer = self.thirdred)
                ]

if __name__ == "__main__":
    start=time.time()
    WordCountTopN.run()
    end=time.time()
    sys.stderr.write(str(end-start))
