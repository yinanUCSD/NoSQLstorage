from Table import Table

t1 = Table()
t1.newtable(cols=['ID', 'IDE', 'OS'], tablepath='./table1/')
t2 = Table()
t2.loadFrom(tablepath='./table2/')

t1.set({'ID':'BOB', 'IDE':'pyCharm', 'OS':'Linux'})

t1.select(cols=['IDE'],
          where={'OS':['Linux', 'Windows'], 'IDE':['Xcode']}
          )



print t1.select(cols=["id","IDE"], where={"language":"python"})
print t1.join(t2)
