

#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import numpy as np
import matplotlib.pyplot as plt


def getState(val):
    itv = 372
    if val > 0*itv and val <= 1*itv:
        return 1
    elif val > 1*itv and val <= 2*itv:
        return 2
    elif val > 2*itv and val <= 3*itv:
        return 3
    elif val > 3*itv and val <= 4*itv:
        return 4
    elif val > 4*itv and val <= 5*itv:
        return 5
    elif val > 5*itv and val <= 6*itv:
        return 6
    elif val > 6*itv and val <= 7*itv:
        return 7
    elif val > 7*itv and val <= 8*itv:
        return 8
    elif val > 8*itv and val <= 9*itv:
        return 9
    elif val > 9*itv and val <= 10*itv:
        return 10
    elif val > 10*itv and val <= 11*itv:
        return 11
    elif val > 11*itv and val <= 4095:
        return 12



#要求  1，如果state有记录，startTime选在 state变为关门的那一刻的前5分钟
#      2，如果state没有记录， startTime选在之前蒸养结束后5分钟
def startEventDetect(dataSet, startTime, tresh):

    time_a = 0
    ts = 5
    j = ts
    
    while dataSet[:,j][0] <= startTime and j<dataSet.shape[1]-1:
        j = j + 1
        

    if dataSet[:,j][3] >= tresh:
        while dataSet[:,j][3] >= tresh and j > ts:
            j = j - 1
            print(dataSet[:,j])
    else:
        pass
    while not (dataSet[:,j][3] > tresh or getState(dataSet[:,j+1][4])*getState(dataSet[:,j][4])==12) and j > ts:
        j = j - 1
    time_a = dataSet[:,j-ts][0]
    
    
    return int(time_a)

#要求  1，如果state有记录，startTime选在 state变为开门的那一刻的后5分钟
#      2，如果state没有记录， startTime选在之前蒸养开始前5分钟
def endEventDetect(dataSet, startTime, tresh):
    
    time_a = 0
    ts = 5
    j = ts
    
    while dataSet[:,j][0] <= startTime and j<dataSet.shape[1]-1:
        j = j + 1
        

    while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] < 0 or dataSet[:,j][3]<= tresh):
        j = j + 1

    while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] >= 0 or dataSet[:,j][3]>tresh):
        j = j + 1

    while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] < 0 or dataSet[:,j][3] <= tresh) and (getState(dataSet[:,j+1][4])*getState(dataSet[:,j][4])!=12):
        j = j + 1

    if (j>=dataSet.shape[1]-2):
        time_a = 0
    else:
        if (dataSet[:,j][3]-dataSet[:,j-1][3] > 0 and dataSet[:,j][3] >= tresh):
            time_a = dataSet[:,j-ts][0]
        
        if (getState(dataSet[:,j+1][4])*getState(dataSet[:,j][4])==12):
            time_a = dataSet[:,j+ts][0]
    
    
    return int(time_a)   


f = open('clave5.txt')
str = f.read()
dataJson = json.loads(str)
f.close()


records = dataJson['records']

#
timeList = []
inTempList =  []
outTempList = []
inPressList = []
stateList = []
length = len(records)

for dataDict in records:
    timeList.append(dataDict['time'])
    inTempList.append(dataDict['inTemp']/100)    
    outTempList.append(dataDict['outTemp']/100)
    inPressList.append(dataDict['inPress']/100)
    stateList.append(dataDict['state'])



dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList],dtype=float)
print(type(dataSet))
print(dataSet.shape)

window = [1/6,1/6,1/6,1/6,1/6,1/6]





shift = 0

fig, axs = plt.subplots(3,1)  # Create a figure containing a single axes.
axs[0].plot(dataSet[0,:],dataSet[3,:])
start = int(len(window)/2)
start = int(len(window)/2)
conv1 = np.convolve(dataSet[1,:], window)
conv2 = np.convolve(dataSet[2,:], window)
conv3 = np.convolve(dataSet[3,:], window)
dataSet[1,:] = conv1[start:start+dataSet.shape[1]]
dataSet[2,:] = conv2[start:start+dataSet.shape[1]]
dataSet[3,:] = conv3[start:start+dataSet.shape[1]]
axs[1].plot(dataSet[0,:], conv3[3:1366])  # Plot some data on the axes.
dif = np.diff(conv3,1)
axs[2].plot(dataSet[0,:], dif[2:1365])

print(startEventDetect(dataSet, 1600338000, 0.05))
plt.show()

for data in np.nditer(dataSet, flags=['external_loop'], order='F'):
    print(data[0])
    print(data[1])
    print(data[2])
    print(data[3])
    print(data[4])
    print('F')

