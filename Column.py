from LRU import LRU
from Bloomfilter import Bloomfilter

class Column:
    def __init__(self, sstablepath=None, colname=None):
        if sstablepath != None:
            self.load(sstablepath)  #load column from sstablefile
        if colname != None:
            self.colname = colname  #build new column


    def get(self, k):
        pass
    def set(self, k, v):
        pass
    def dumpMem(self):
        pass
    def getSucc(self, k, v):
        pass
    def close(self):
        pass