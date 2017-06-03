from Column import Column
import glob
class Table:
    def __init__(self):
        self.columns = {}
    def loadFrom(self, tablepath):
        for colfile in glob.glob(tablepath + '*.sstb'):
            column = Column()
            column.loadFrom(colfile)
            self.columns[column.colname] = column
            self.key = column.keyname
    def newtable(self, cols, tablepath, compression=None):
            key=cols[0]
            self.key = key
            for col in cols[1:]:
                column = Column()
                column.newColumn(colname=col, keyname=key, compression=compression, grouppath=tablepath)
                self.columns[col] = column
    def save(self):
        for col in self.columns:
            self.columns[col].save()
    def set(self, rowDict):
        for colname in rowDict:
            if colname == self.key:
                continue
            self.columns[colname].set(rowDict[self.key], rowDict[colname])
    def select(self, cols, where):
        keys = set()
        if self.key in where:
            keys.add(self.key)
        for colname in where:
            if colname == self.key:
                continue
            keys = self.columns[colname].listKeys(values=where[colname], keysDomain=keys)
        res = []
        for key in keys:
            row = []
            for col in cols:
                row.append(self.columns[col].get(key))
            res.append(row)
        return res

    def join(self, other):
        pass

