from Table import Table
import time
import random
t1 = Table()
genderSet = ['male', 'female']
osSet = ['linux', 'windows', 'macOS']
t1.newtable(cols=['ID', 'gender', 'OS', 'salary'], tablepath='./table1/', tablename='t1', compressed=0)
for i in range(2001):
    t1.set({'ID': str(i), 'gender': genderSet[i % len(genderSet)], 'OS': osSet[i % len(osSet)], 'salary': str(i*100)})
for i in range(2001):
    if not t1.hasKey(str(i)):
        print i
'''
t1 = Table()
t1.newtable(cols=['ID', 'gender', 'OS', 'salary'], tablepath='./table1/', tablename='t1', compressed=0)
t2 = Table()
t2.newtable(cols=['ID', 'countryNo', 'phoneNo'], tablepath='./table2/', tablename='t2', compressed=0)
genderSet = ['male', 'female']
osSet = ['linux', 'windows', 'macOS']

starttime = time.time()
for i in range(10000):
    t1.set({'ID': str(i), 'gender': genderSet[i % len(genderSet)], 'OS': osSet[i % len(osSet)], 'salary': str(i*100)})
endtime = time.time()
print "time1: ", endtime - starttime


starttime = endtime
for i in range(20000):
    t2.set({'ID': str(i), 'countryNo': str(random.random()), 'phoneNo': str(random.random())})
endtime = time.time()
print "time2: ", endtime - starttime

starttime = endtime
print len(t1.select(cols=["OS", "salary"], where={"gender": "male"}))
endtime = time.time()
print "time3: ", endtime - starttime

starttime = endtime
print t1.hasKey('10')
endtime = time.time()
print "time4: ", endtime - starttime


starttime = endtime
print t1.sum('salary')
endtime = time.time()
print "time6: ", endtime - starttime

starttime = endtime
print t1.max('salary')
endtime = time.time()
print "time7: ", endtime - starttime

starttime = endtime
print t1.count('gender', 'female')
endtime = time.time()
print "time8: ", endtime - starttime



starttime = endtime


print t1.hasKey('12')

t1.save()
t2.save()

print "t3 begin"
t3 = Table()
t3.loadFrom(tablepath='./table1/', compressed=0)

print t3.select(cols=["OS", "salary"], where={"gender": "male"})[:5]
'''