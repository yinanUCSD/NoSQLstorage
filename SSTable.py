class SSTable:
    path = ""
    f = ""
    def __init__(self, sstablepath=None):
        if sstablepath != None:
            self.path = sstablepath
    # base file operation
    def open(self, path="", mode='r'):
        if path=="":
            path = self.path
        self.f = open(path,mode)
    def close(self):
        self.f.close()
    def tell(self):
        return self.f.tell()
    def seek(self,offset):
        self.f.seek(offset)
    def readline(self):
        return self.f.readline()
    def readlines(self):
        return self.f.readlines()
    def read(self):
        return self.f.read()

    # extra operation
    def getpath(self):
        return self.path
    def getkv(self,offset=0):
        self.f.seek(offset)
        return self.f.readline().strip().split(',')
    def getk(self,offset=0):
        self.f.seek(offset)
        return self.f.readline().strip().split(',')[0]
    def getv(self,offset=0):
        self.f.seek(offset)
        return self.f.readline().strip().split(',')[1]