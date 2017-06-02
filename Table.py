from Column import Column
class Table:
    def __init__(self, cols=None, tablePath=None):
        if cols != None:      #build new table
            for col in cols:
                self.cols[col] = Column(colname=col)
        if tablePath != None:  #load table from file
            self.load(tablePath)
    def load(self, tablePath):
        pass
    def insert(self, tuple):
        pass
    def select(self, cols, where):
        pass
    def join(self, other):
        pass

