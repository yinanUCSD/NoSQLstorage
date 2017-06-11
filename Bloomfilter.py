from pybloom import BloomFilter

class Bloomfilter:
    bf = ""
    def __init__(self,capacity=100000, error_rate=0.001):
        self.bf = BloomFilter(capacity=capacity, error_rate=error_rate)
    def add(self, k):
        self.bf.add(k)
    def find(self, k):
        return k in self.bf