import storm
import numpy as np

class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
        a = np.array([0,1,2,3,4,5])
        words = tup.values[0].split(" ")
        for word in words:
          storm.emit([word])

SplitSentenceBolt().run()