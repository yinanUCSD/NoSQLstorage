import collections

size = 10000

class LRU:
    cache = {}
    def __init__(self):
        self.size = size
        self.cache = collections.OrderedDict()

    def has(self, k):
        if self.cache.has_key(k):
            return True
        else :
            return False

    def get(self, k):
        if self.cache.has_key(k):
            v = self.cache.pop(k)
            self.cache[k] = v
            return v
        else:
            v = None
            return v

    def update(self, k, v):
        if self.cache.has_key(k):
            self.cache.pop(k)
            self.cache[k] = v
        elif self.size == len(self.cache):
            self.cache.popitem(last=False)
            self.cache[k] = v
        else:
            self.cache[k] = v