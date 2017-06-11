from LRU import LRU
from Bloomfilter import Bloomfilter
from SSTable import SSTable
import os
import os.path
import zlib

M = 2000
B = 100

class Column:
    def __init__(self):
        self.indextable = {}
        self.memtable = {}
        self.bloomfilter = Bloomfilter()
        self.LRU = LRU()

    def loadFrom(self, sstablepath, compressed):
        if not os.path.exists(sstablepath):
            fout = open(sstablepath,'w')
            fout.write(self.keyname+','+self.colname+','+self.groupname+'\n')
            fout.close()
        self.sstable = SSTable(sstablepath,compressed)
        self.sstable.open(sstablepath, 'r')
        self.sstablepath = sstablepath
        self.compressed = compressed
        # first line should be metadata: keyname, colname, groupname
        firstline = self.sstable.readline().strip().split(',')
        self.keyname,self.colname,self.groupname = firstline[0], firstline[1], firstline[2]  # metadata
        while True:
            offset = self.sstable.tell()
            block = self.sstable.readblock()
            # eof
            if len(block) == 0:
                break
            #
            for i in range(len(block)):
                k, v = block[i].strip().split(',')
                self.indextable[k] = offset
                self.bloomfilter.add(k)
            # eof
            if len(block) < B:
                break
            #

        self.sstable.close()

    def newColumn(self, colname, keyname, compressed, grouppath, groupname):
        self.colname = colname  # build new column
        self.keyname = keyname
        self.grouppath = grouppath
        self.groupname = groupname
        self.compressed = compressed
        sstablepath = grouppath+groupname+'_'+colname+'.sstb'
        self.sstable = SSTable(sstablepath,compressed)

        fout = open(sstablepath, 'w')
        fout.write(self.keyname + ',' + self.colname + ',' + self.groupname + '\n')
        fout.close()

    def save(self):
        memtable1 = self.memtable
        self.dumpMem(memtable1)
    def get(self, k):
        if k in self.memtable:
            if self.memtable[k]=='NULL':
                return "not found"
            return self.getSucc(k,self.memtable[k])
        else:
            if self.bloomfilter.find(k) == False:
                return "not found"
            else:
                if self.LRU.has(k) == True:
                    return self.getSucc(k, self.LRU.get(k))
                else:
                    if k not in self.indextable:
                        return "not found"
                    else:
                        offset = self.indextable[k]
                        self.sstable.open(mode='r')
                        v = self.sstable.getv(offset,k)
                        self.sstable.close()
                        return self.getSucc(k, v)

    def set(self, k, v):
        self.memtable[k] = v
        self.bloomfilter.add(k)
        self.LRU.update(k, v)
        if v=="NULL":
            self.indextable.pop(k)
        if len(self.memtable) >= M:
            memtable1 = self.memtable
            self.dumpMem(memtable1)  # asynchronously dump
            self.memtable = {}  # can't use memtable.clear() here, otherwise the dumping object will be cleared
            # assumption:
            # we dont use lock on memtable, assume that there is only one thread giving get() and set()
        return True
    def listKeys(self, values=None, keysDomain=None):
        #list keys where value exist in set values and key is in keysDomain, if keysDomain is empty, it means keysDomain = ALL
        #if values is empty, list all keys
        keyList = []
        if values == None or len(values)==0:
            return list(set(self.indextable.keys() + self.memtable.keys()))
        # memtable
        if keysDomain == None or len(keysDomain)==0:
            for k, v in self.memtable.items():
                if v in values:
                    keyList.append(k)
        else :
            for k, v in self.memtable.items():
                if v in values and k in keysDomain:
                    keyList.append(k)
        # sstable
        self.sstable.open(mode='r')
        self.sstable.readline0()  # metadata
        while True:
            line = self.sstable.readline()
            if not line:
                break
            k, v = line.strip().split(',')
            if keysDomain == None or len(keysDomain)==0:
                if v in values and not self.memtable.has_key(k):
                    keyList.append(k)
            else :
                if v in values and k in keysDomain and not self.memtable.has_key(k):
                    keyList.append(k)
        self.sstable.close()
        return keyList

    def count(self, value):
        count = 0
        for k, v in self.memtable.items():
            if v == value:
                count += 1

        self.sstable.open(mode='r')
        self.sstable.readline0()  # metadata
        while True:
            line = self.sstable.readline()
            if not line:
                break
            k, v = line.strip().split(',')
            if v == value and not self.memtable.has_key(k):
                count += 1
        self.sstable.close()
        return count

    def sum(self):
        sums = 0.
        for k, v in self.memtable.items():
            if v != "Null":
                sums += float(v)

        self.sstable.open(mode='r')
        self.sstable.readline0()  # metadata
        while True:
            line = self.sstable.readline()
            if not line:
                break
            k, v = line.strip().split(',')
            if not self.memtable.has_key(k):
                sums += float(v)            # no tomb in sstable
        self.sstable.close()
        return sums

    def max(self):
        maxs = float("-inf")
        # memtable
        for k, v in self.memtable.items():
            if v != "Null":
                maxs = max(maxs,float(v))

        self.sstable.open(mode='r')
        self.sstable.readline0()    # metadata
        while True:
            line = self.sstable.readline()
            if not line:
                break
            k, v = line.strip().split(',')
            if not self.memtable.has_key(k):
                maxs = max(maxs,float(v))  # no tomb in sstable
        self.sstable.close()
        if maxs==float("-inf"):
            return "NULL"
        return maxs

    def dumpMem0(self,memtable1):
        # dump
        keys = memtable1.keys()
        keys.sort()

        fout = open('tmp', 'w')
        fout.write(self.keyname+','+self.colname+','+self.groupname+'\n')
        self.sstable.open(mode='r')

        line = self.sstable.readline()  # metadata
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
        os.remove(self.sstable.getpath())
        os.rename('tmp',self.sstable.getpath())

        # update indextable (tomb will be poped when set "NULL")
        self.sstable.open(mode='r')
        self.sstable.readline()  # metadata
        while True:
            offset = self.sstable.tell()
            line = self.sstable.readline()
            # eof
            if not line:
                break
            #
            k, v = line.strip().split(',')
            self.indextable[k] = offset

    def dumpMem(self,memtable1):
        # dump
        keys = memtable1.keys()
        keys.sort()

        fout = SSTable('tmp',compressed=self.compressed)
        fout.open(mode='w')
        fout.write0(self.keyname+','+self.colname+','+self.groupname+'\n')  # metadata
        self.sstable.open(mode='r')

        line = self.sstable.readline0()  # metadata
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
        os.remove(self.sstable.getpath())
        os.rename('tmp',self.sstable.getpath())

        # update indextable (tomb will be poped when set "NULL")
        self.sstable.open(mode='r')
        self.sstable.readline0()  # metadata
        while True:
            offset = self.sstable.tell()
            block = self.sstable.readblock()
            #print block
            # eof
            if len(block)==0:
                break
            #
            for i in range(len(block)):
                k,v = block[i].strip().split(',')
                self.indextable[k] = offset
            # eof
            if len(block)<B:
                break
            #
        self.sstable.close()

    def getSucc(self, k, v):
        self.LRU.update(k, v)
        return v

    def close(self):
        memtable1 = self.memtable
        self.dumpMem(memtable1)
