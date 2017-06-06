from Column import Column
import glob
import os
class Table:
    def __init__(self):
        self.columns = {}
    def loadFrom(self, tablepath):
        for colfile in glob.glob(tablepath + '*.sstb'):
            column = Column()
            column.loadFrom(colfile)
            self.columns[column.colname] = column
            self.key = column.keyname
            self.tablename = column.groupname

    def newtable(self, cols, tablepath, tablename, compression=None):
            key=cols[0]
            self.key = key
            self.tablename = tablename
            if not os.path.exists(tablepath):
                os.mkdir(tablepath)
            for col in cols[1:]:
                column = Column()
                column.newColumn(colname=col, keyname=key, compression=compression, grouppath=tablepath, groupname=tablename)
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
                if col == self.key:
                    row.append(key)
                    continue
                row.append(self.columns[col].get(key))
            res.append(row)
        return res

    def hasKey(self, k):
        if self.columns[self.columns.keys()[0]].get(k) != 'not found':
            return True
        else:
            return False

    def join(self, other):
        selfKeys = self.columns[self.columns.keys()[0]].listKeys()
        res = {}
        for k in selfKeys:
            if not other.hasKey(k):
                continue
            res[k] = {}
            for col in self.columns:
                res[k][self.tablename + col] = self.columns[col].get(k)
            for col in other.columns:
                res[k][other.tablename + col] = other.columns[col].get(k)
        return res

    def count(self, colname, value):
        return self.columns[colname].count(value)
    def sum(self, colname):
        return self.columns[colname].sum()
    def max(self, colname):
        return self.columns[colname].max()





