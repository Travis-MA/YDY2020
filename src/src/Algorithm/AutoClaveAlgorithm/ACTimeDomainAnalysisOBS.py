# -*- coding:utf-8 -*-
import json
import numpy as np
from model.Algorithm import Algorithm
from model.Data import Data
from src.Data import AutoClaveRecordData
from configparser import ConfigParser
from datetime import datetime
from datetime import timezone
from datetime import timedelta

class ACTimeDomainAnalysisOBS(Algorithm):

    def __init__(self, OBSTool, dataObj, realTimeRecord):

        self.OBSTool = OBSTool
        self.dataObj = dataObj
        self.realTimeRecord = realTimeRecord

    
    def getType(self):
        return "ACTimeDomainAnalysisOBS"

    def run(self):
        total = 0
        for claveId in range(1,self.realTimeRecord.getClaveNum()+1):
            #print('for clave:'+str(claveId))
            startTime = 0
            oldState = ""

            #找到最新纪录时间，分两种情况，1是全部是FIN，所以开始时间算作最后一个FIN的结束时间
            #2是有ING，有且仅有一个ING，开始时间算作ING的开始时间
            for event in self.dataObj.getSet(claveId).getSet():
                prefix = event.getPrefix()
                Xindex = prefix.find("X")
                Yindex = prefix.find("Y")
                if(prefix[Xindex+1:Xindex+4] == "ING"):
                    time = int(prefix[Xindex+4:Yindex])
                    startTime = time
                    oldState = "ING"
                    break
                elif(prefix[Xindex+1:Xindex+4] == "FIN"): 
                    time = int(prefix[Yindex+1:])
                    if time>startTime:
                        startTime = time
                        oldState = "FIN"

            #实时record list
            dataSet = self.realTimeRecord.getSet(claveId).getSet('numpy')

            print('ClaveId '+str(claveId)+' sttime: '+str(startTime)+'  oldState: '+oldState)
            if dataSet[0].all() != 0:
                #如果上一次记录的state是FIN， 要找新的事件, 没有新的事件则不录
                time = 0
                if oldState == 'FIN':
                    time = self.__startEventDetect(dataSet, startTime, 0.05)
                    if(time > 0):
                        ev = self.dataObj.newEvent('XING',claveId)
                        ev = self.__writeContent(claveId, dataSet,ev,time,0)
                        print('evPrefix newIng: '+ev.getPrefix()+' time '+str(time))
                        self.dataObj.getSet(claveId).pushData(ev)

                #如果上一次记录的state是ING，则判断是否结束， 若没有结束就更新数据， 有结束则新建FIN
                elif oldState == 'ING':
                    time = self.__endEventDetect(dataSet, startTime, 0.05)

                    ev = self.dataObj.newEvent('XFIN', claveId)
                    ev = self.__writeContent(claveId, dataSet,ev,startTime,time)
                    print('New FIN/Ref ING: '+ev.getPrefix()+' time '+str(time))
                    self.dataObj.getSet(claveId).pushData(ev)

                    if time == 0:
                        total = total + 1

        ifAllEnd = 0
        if total == self.realTimeRecord.getClaveNum():
            ifAllEnd = 1

        return self.dataObj, ifAllEnd


    def __writeContent(self, claveId, dataSet, event, startTime, endTime):
        if event.getType()=='SingleAutoClaveRecordEvent':
            event.setStartTime(startTime)
            event.setEndTime(endTime)

            if(endTime <= 0):
                endTime = 1000000000000000
                event.setPrefix(str(claveId)+"XING"+str(startTime)+"Y")
            else:
                event.setPrefix(str(claveId)+"XFIN"+str(startTime)+"Y"+str(endTime))
            
            for data in np.nditer(dataSet, flags=['external_loop'], order='F'):

                if data[0] >= startTime and data[0] <= endTime:
                    dataObj = AutoClaveRecordData(event.getClaveId(), data[0])

                    dataObj.setInTemp(data[1])
                    dataObj.setOutTemp(data[2])
                    dataObj.setInPress(data[3])
                    dataObj.setState(self.__getState(data[4]))
     
                    event.pushData(dataObj)
            
            return event
        else:
            print('DataType Error')

#要求  1，如果state有记录，startTime选在 state变为关门的那一刻的前5分钟
#      2，如果state没有记录， startTime选在之前蒸养结束后5分钟
    def __startEventDetect(self, dataSet, startTime, tresh):

        time_a = 0
        ts = 5
        j = ts
        
        while dataSet[:,j][0] <= startTime and j<dataSet.shape[1]-1:
            j = j + 1

        if dataSet[:,j][3] >= tresh:
            while dataSet[:,j][3] >= tresh and j > ts:
                j = j - 1
        else:
            pass
        while not (dataSet[:,j][3] > tresh or self.__getState(dataSet[:,j+1][4]) != self.__getState(dataSet[:,j][4])) and j > ts:
            j = j - 1
        time_a = dataSet[:,j-ts][0]
              
        return int(time_a)
    

    #要求  1，如果state有记录，startTime选在 state变为开门的那一刻的后5分钟
    #      2，如果state没有记录， startTime选在之前蒸养开始前5分钟
    def __endEventDetect(self, dataSet, startTime, tresh):
        
        time_a = -1
        ts = 5
        j = ts
        
        while dataSet[:,j][0] <= startTime and j<dataSet.shape[1]-1:
            j = j + 1
            
        while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] < 0 or dataSet[:,j][3]<= tresh):
            j = j + 1

        while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] >= 0 or dataSet[:,j][3]>tresh):
            j = j + 1

        while j<dataSet.shape[1]-1 and (dataSet[:,j][3]-dataSet[:,j-1][3] < 0 or dataSet[:,j][3] <= tresh) and (self.__getState(dataSet[:,j+1][4])*self.__getState(dataSet[:,j][4])!=12):
            j = j + 1

        if (j>=dataSet.shape[1]-2):
            time_a = 0

        else:
            if (dataSet[:,j][3]-dataSet[:,j-1][3] > 0 and dataSet[:,j][3] >= tresh):
                time_a = dataSet[:,j-ts][0]
            
            if (self.__getState(dataSet[:,j+1][4])*self.__getState(dataSet[:,j][4])==12):
                time_a = dataSet[:,j+ts][0]
              
        return int(time_a)  

    def __getState(self, val):
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
