from Column import Column
class Table:
    def __init__(self, cols=None, tablePath=None):
        if cols != None:
            for col in cols:
                self.cols[col] = Column(colname=col)
        if tablePath != None:
            self.load(tablePath)
    def load(self, tablePath):  #load table from file
        pass
    def insert(self, tuple):
        pass
    def select(self, cols, where):
        pass
    def join(self, other):
        pass

