class SSTable:
    def __init__(self, sstablepath=None):
        if sstablepath != None:
            self.path = sstablepath
