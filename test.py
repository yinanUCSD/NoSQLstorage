from Table import Table

t1 = Table(cols=["id", "IDE", "language"])
t2 = Table(cols=["id", "IDE", "OS"])
t1.insert(["bob", "pycharm", "python"])
t2.insert(["alice", "xcode", "macOS"])
print t1.select(cols=["id","IDE"], where={"language":"python"})
print t1.join(t2)
