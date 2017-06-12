from Table import Table
import time
import random
from matplotlib import pyplot as plt
from Column import M


genderSet = ['male', 'female']
osSet = ['linux', 'windows', 'macOS']
setfile = open('settime', 'w')
selectfile = open('selecttime', 'w')
sumfile = open('sumtime', 'w')
haskeyfile = open('haskeytime', 'w')
nokeyfile = open('nokeytime', 'w')
sett = []
selectt = []
sumt = []
haskeyt = []
nokeyt = []
joint = []

nums = [M / 10, M / 7, M / 5, M / 3, M / 2, M * 2, M * 3, M * 5, M * 7, M * 10]
t2 = Table()
t2.newtable(cols=['ID', 'game'], tablepath='./table2/', tablename='t2', compressed=0)
for i in range(10):
    t2.set({'ID': str(i), 'game': 'GTA'})

for num in nums:
    print "********************************************************"
    print "start testting for table with record number: ", num
    t1 = Table()
    t1.newtable(cols=['ID', 'gender', 'OS', 'salary'], tablepath='./table' + str(num) + '/', tablename='t1', compressed=0)
    starttime = time.time()
    for i in range(num):
        t1.set({'ID': str(i), 'gender': genderSet[i % len(genderSet)], 'OS': osSet[i % len(osSet)], 'salary': str(i % 10000)})
    endtime = time.time()
    t = endtime - starttime
    sett.append(t)
    setfile.write(str(t) + '\n')
    print "time for set: ", t

    starttime = time.time()
    print len(t1.select(cols=["OS", "salary"], where={"salary": ['100', '200', '500']}))
    endtime = time.time()
    print "time for select: ", endtime - starttime
    t = endtime - starttime
    selectt.append(t)
    selectfile.write(str(t) + '\n')

    starttime = time.time()
    print t1.sum('salary')
    endtime = time.time()
    print "time for sum: ", endtime - starttime
    t = endtime - starttime
    sumt.append(t)
    sumfile.write(str(t) + '\n')

    starttime = time.time()
    t1.join(t2)
    endtime = time.time()
    print "time for join: ", endtime - starttime
    t = endtime - starttime
    joint.append(t)

    starttime = time.time()
    print t1.hasKey('10')
    endtime = time.time()
    print "time for hasKey exist: ", endtime - starttime
    t = endtime - starttime
    haskeyt.append(t)
    haskeyfile.write(str(t) + '\n')

    starttime = time.time()
    print t1.hasKey('-1')
    endtime = time.time()
    print "time for hasKey not exist: ", endtime - starttime
    t = endtime - starttime
    nokeyt.append(t)
    nokeyfile.write(str(t) + '\n')

data = [sett, selectt, sumt, haskeyt, nokeyt, joint]
title = ['set', 'select', 'sum', 'haskey', 'nokey', 'join']
plt.figure(1)
for i in range(len(data)):
    plt.subplot(2, 3, i + 1)
    plt.plot(nums[:len(nums)/2], data[i][:len(nums)/2])
    plt.title('time for ' + title[i])
    plt.grid(True)
    plt.subplots_adjust(top=0.92, bottom=0.08, left=0.10, right=0.95, hspace=0.25,
                        wspace=0.35)
    plt.ylim(ymin=0)
plt.figure(2)
for i in range(len(data)):
    plt.subplot(2, 3, i + 1)
    plt.plot(nums[len(nums)/2:], data[i][len(nums)/2:])
    plt.title('time for ' + title[i])
    plt.grid(True)
    plt.subplots_adjust(top=0.92, bottom=0.08, left=0.10, right=0.95, hspace=0.25,
                        wspace=0.35)
    plt.ylim(ymin=0)
plt.figure(3)
plt.plot(nums[:len(nums)/2], haskeyt[:len(nums)/2], 'r', nums[:len(nums)/2], nokeyt[:len(nums)/2], 'b')

plt.ylim(ymin=0)
plt.figure(4)
plt.plot(nums[len(nums)/2:], haskeyt[len(nums)/2:], 'r', nums[len(nums)/2:], nokeyt[len(nums)/2:], 'b')
plt.ylim(ymin=0)
plt.figure(5)
plt.plot(nums[:len(nums)/2 + 1], sett[:len(nums)/2 + 1])
plt.ylim(ymin=0)

plt.show()
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