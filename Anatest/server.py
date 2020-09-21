

#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import numpy as np
import matplotlib.pyplot as plt

f = open('clave5.txt')
str = f.read()
dataJson = json.loads(str)
f.close()


records = dataJson['records']


timeList = []
inTempList =  []
outTempList = []
inPressList = []
stateList = []
length = len(records)

for dataDict in records:
    timeList.append(int(dataDict['time'])/1000)
    inTempList.append(dataDict['inTemp'])    
    outTempList.append(dataDict['outTemp'])
    inPressList.append(dataDict['inPress'])
    stateList.append(dataDict['state'])



dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList],dtype=float)
print(type(dataSet))
print(dataSet.shape)

window = [1,1,1,1,1,1]


conv = np.convolve(dataSet[3,:], window)
dif = np.diff(conv,2)

shift = 0

fig, axs = plt.subplots(3,1)  # Create a figure containing a single axes.
axs[0].plot(dataSet[0,:],dataSet[3,:])
axs[1].plot(dataSet[0,:], conv[3:1366])  # Plot some data on the axes.
axs[2].plot(dataSet[0,:], dif[2:1365])
plt.show()
