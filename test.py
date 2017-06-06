from Table import Table

t1 = Table()
t1.newtable(cols=['ID', 'gender', 'OS', 'salary'], tablepath='./table1/', tablename='t1')
t2 = Table()
t2.loadFrom(tablepath='./table2/')

t1.set({'ID': 'bob', 'gender': 'male', 'OS': 'Linux', 'salary':'1000'})
t1.set({'ID': 'alice', 'gender': 'female', 'OS': 'macOS','salary':'2000' })
t1.set({'ID': 'charlie', 'gender': 'male', 'OS': 'windows', 'salary':'3000'})


#print t1.select(cols=["ID", "OS"], where={"gender": "male"})
print t1.select(cols=["OS", "salary"], where={"gender": "male"})

print t1.join(t2)
print t1.sum('salary')
print t1.max('salary')
print t1.count('gender', 'female')
