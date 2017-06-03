from LRU import LRU
from Bloomfilter import Bloomfilter
from SSTable import SSTable
import os
import os.path

class Column:
    M = 10000
    colname = ""
    indextable = {}
    memtable = {}
    sstable = ""
    def __init__(self, sstablepath=None, colname=None, compression=None):
        if sstablepath != None:
            self.sstable = SSTable(sstablepath)
            if compression == None:
                self.sstable.open(sstablepath,'r')
                while True:
                    offset = self.sstable.tell()
                    line = self.sstable.readline()
                    # eof
                    if not line:
                        break
                    #
                    k,v = line.strip().split()
                    self.indextable[k] = offset
                    self.bloomfilter.add(k)
                    self.memtable = {}
                self.sstable.close()
        else:
            self.sstable = SSTable("")
        if colname != None:
            self.colname = colname  #build new column

    def get(self, k):
        if k in self.memtable:
            if self.memtable[k]=='NULL':
                return "not found"
            return getSucc(k, memtable[k])
        else:
            if self.bloomfilter.find(k) == false:
                return "not found"
            else:
                if self.LRU.has(k) == True:
                    return getSucc(k, self.LRU.find(k))
                else:
                    if self.indextable.has(k) == False:
                        return "not found"
                    else:
                        offset = self.indextable.find(k)
                        self.sstable = open(mode='r')
                        v = self.sstable.getv(offset)
                        self.sstable.close()
                        return getSucc(k, v)

    def set(self, k, v):
        self.memtable[k] = v
        self.bloomfilter.add(k)
        self.LRU.update(k, v)
        if v=="NULL":
            indextable.pop(k)
        if self.memtable.size() > M:
            memtable1 = self.memtable
            self.dumpMem(memtable1)  # asynchronously dump
            self.memtable = {}  # can't use memtable.clear() here, otherwise the dumping object will be cleared
            # assumption:
            # we dont use lock on memtable, assume that there is only one thread giving get() and set()
        return True

    def dumpMem(self,memtable1):
        # dump
        keys = memtable1.keys().sort()

        fout = open('tmp', 'w')
        self.sstable.open(mode='r')

        line = self.sstable.readline()
        i = 0
        while True:
            # memtable ends
            if i>=len(keys):
                if not line:    # both ends
                    break
                k2,v2 = line.strip().split(',')
                fout.write(k2 + ',' + v2 + '\n')
                line = self.sstable.readline()
                continue
            k1, v1 = keys[i], memtable1[keys[i]]
            # sstable ends
            if not line:
                if v1!="NULL":
                    fout.write(k1+ ','+v1+'\n')
                i += 1
                continue
            k2, v2 = line.strip().split(',')
            # none ends
            if k1==k2:  # throw k2,v2, write k1,v1
                if v1!="NULL":
                    fout.write(k1+ ','+v1+'\n')
                i += 1
                line = self.sstable.readline()
            elif k1<k2:
                if v1!="NULL":
                    fout.write(k1+ ','+v1+'\n')
                i += 1
            elif k1>k2:
                fout.write(k2+','+v2+'\n')
                line = self.sstable.readline()
        self.sstable.close()
        fout.close()
        os.remove(sstable.getpath())
        os.rename('tmp',sstable.getpath())

        # update indextable (tomb will be poped when set "NULL")
        self.sstable.open(mode='r')
        while True:
            offset = self.sstable.tell()
            line = self.sstable.readline()
            # eof
            if not line:
                break
            #
            k, v = line.strip().split()
            self.indextable[k] = offset

    def getSucc(self, k, v):
        self.LRU.update(k, v)
        return v

    def close(self):
        memtable1 = self.memtable
        dumpMem(memtable1)
