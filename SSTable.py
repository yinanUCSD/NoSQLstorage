import zlib

B = 100

class SSTable:
    def __init__(self, sstablepath=None,compressed=0):
        if sstablepath != None:
            self.path = sstablepath
        self.f = ""
        self.block = []
        self.blk_end = 1
        self.offset = 0
        self.compressed = compressed
    # base file operation
    def open(self, path="", mode='r'):
        if path=="":
            path = self.path
        self.mode = mode
        self.f = open(path,mode)
    def close(self):
        if self.mode == 'w' and len(self.block)!=0:
            block = '\n'.join(self.block) + '\n'
            if self.compressed == 1:
                block = zlib.compress(block)
            self.f.write(block)
        self.f.close()
    def tell(self):
        return self.f.tell()
    def seek(self,offset):
        self.f.seek(offset)
    def readlines(self):
        return self.f.readlines()
    def read(self):
        return self.f.read()
    def readline0(self):
        return self.f.readline()
    def readline(self):
        if self.compressed==0:
            return self.f.readline()
        else:
            if len(self.block)==0:
                block = self.f.readline()
                if not block:
                    return block  # eof, block=''
                self.block = zlib.decompress(block).split()
            line = self.block[self.offset]
            self.offset += 1
            if self.offset>=len(self.block):
                self.offset = 0
                self.block = []
            return line

    def readblock(self):
        self.block = []
        if self.compressed==0:
            for i in range(B):
                line = self.f.readline()
                if not line:
                    return self.block
                self.block.append(line.strip())
            return self.block
        else:
            block = self.f.readline()
            if not block:
                return []   # eof, block=''
            print block,1
            self.block = zlib.decompress(block).split()
            return self.block

    # write operation
    def write0(self,text):
        self.f.write(text)
    def write(self,text):
        self.block.append(text.strip())
        self.offset += 1
        if self.offset >= B:
            self.offset = 0
            block = '\n'.join(self.block) + '\n'
            if self.compressed==1:
                block = zlib.compress(block)+'\n'
            self.f.write(block)
            self.block = []

    # extra operation
    def getpath(self):
        return self.path
    def getkv(self,offset=0):
        self.f.seek(offset)
        return self.f.readline().strip().split(',')
    def getk(self,offset=0):
        self.f.seek(offset)
        return self.f.readline().strip().split(',')[0]
    def getv(self,offset=0,key=''):
        self.f.seek(offset)
        if self.compressed==0:
            block = self.readblock()
            for i in range(len(block)):
                k,v = block[i].strip().split(',')
                if k==key:
                    return v