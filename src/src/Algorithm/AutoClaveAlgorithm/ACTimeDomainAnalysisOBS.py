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
        for claveId in range(1,self.realTimeRecord.getClaveNum()+1):
            print('for clave:'+str(claveId))
            startTime = 0
            oldState = ""
            #找到最新纪录时间
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

            recordList = self.realTimeRecord.getSet(claveId).getSet('list')
            
            if oldState == 'FIN':
                time = self.__startEventDetect(recordList, startTime, 0.05)
                if(time > 0):
                    ev = self.dataObj.newEvent('XING',claveId)
                    ev = self.__writeContent(claveId, recordList,ev,time,0)
                    ev = self.__stateDetect(recordList,ev)
                    self.dataObj.getSet(claveId).pushData(ev)
            
            elif oldState == 'ING':
                time = self.__endEventDetect(recordList, startTime, 0.05)
                event = self.__writeContent(claveId, recordList,event,startTime,time)
                event = self.__stateDetect(recordList, event)

            #self.__toNumPy(recordList)

        return self.dataObj



    def __toNumPy(self, recordList):
        #得到实时数据
        timeList = []
        inTempList =  []
        outTempList = []
        inPressList = []
        stateList = []

        for autoClaveData in recordList:
            time = int(autoClaveData.getTime())

            inTemp = autoClaveData.getInTemp()
            outTemp = autoClaveData.getOutTemp()
            inPress = autoClaveData.getInPress()
            state = autoClaveData.getState()

            timeList.append(time)
            inTempList.append(inTemp)
            outTempList.append(outTemp)
            inPressList.append(inPress)
            stateList.append(state)


                #print("Id:"+str(claveId)+" T:"+str(time)+" iT:"+str(inTemp)+" oT:"+str(outTemp)+" iP:"+str(inPress)+" S:"+str(state))

        dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList])
        print(dataSet)

    def __writeContent(self, claveId, recordList, event, startTime, endTime):

        if event.getType()=='SingleAutoClaveRecordEvent':
            event.setStartTime(startTime)
            event.setEndTime(endTime)

            if(endTime == 0):
                endTime = 1000000000000000
                event.setPrefix(str(claveId)+"XING"+str(startTime)+"Y")
            else:
                event.setPrefix(str(claveId)+"XFIN"+str(startTime)+"Y"+str(endTime))

            for autoClaveData in recordList:
                time = int(autoClaveData.getTime())
                if time >= startTime and time <= endTime:
                    dataObj = AutoClaveRecordData(event.getClaveId(), time)

                    dataObj.setInTemp(autoClaveData.getInTemp(), autoClaveData.getInTempDiff())

                    dataObj.setOutTemp(autoClaveData.getOutTemp(), autoClaveData.getOutTempDiff())

                    dataObj.setInPress(autoClaveData.getInPress(), autoClaveData.getInPressDiff())

                    dataObj.setState(autoClaveData.getState(), self.__getState(autoClaveData.getState()))
     
                    event.pushData(dataObj)
            
            return event
        else:
            print('DataType Error')


    def __startEventDetect(self, recordList, startTime, tresh):

        time = 0
        for i in range(1,len(recordList)):
            if time == 0:
                autoClaveData = recordList[i]
                rectime = int(autoClaveData.getTime())
                if(rectime > startTime):
                    if autoClaveData.getInPress() >= tresh:
                        for j in range(2,i-2):
                            k = i-j

                            rec = [recordList[k-2].getInPress(), recordList[k-1].getInPress(), recordList[k].getInPress(), recordList[k+1].getInPress(), recordList[k+2].getInPress()]

                            if rec[2] < tresh and self.__getState(recordList[k].getState())=='S1' and self.__getState(recordList[k-1].getState())=='S12':
                                time = recordList[k-1].getTime()
                                break

                            if rec[0] > tresh and rec[1] > tresh and rec[2] > tresh and rec[3] <= tresh and rec[4] <= tresh:
                                time = recordList[k].getTime()
                                break
                    else:
                        time = startTime        

            else:
                pass
        return int(time)
        

    def __endEventDetect(self, recordList, startTime, tresh):
        time = 0
        flag = 0
        first = 0
        for i in range(2,len(recordList)-2):
            if time == 0:
                autoClaveData = recordList[i]   
                rectime = int(autoClaveData.getTime())
                if(rectime > startTime):
                    if flag == 0 and recordList[i].getInPress() >= tresh:
                        first = 1
                    flag = 1

                    rec = [recordList[i-2].getInPress(), recordList[i-1].getInPress(), recordList[i].getInPress(), recordList[i+1].getInPress(), recordList[i+2].getInPress()]

                    if rec[4] >= tresh and rec[3] >= tresh and rec[2] >= tresh and rec[1] < tresh and rec[0] < tresh:
                        if first == 0:
                            first = 1
                        else:
                            time = recordList[i].getTime()

                    if self.__getState(recordList[i-1].getState())=='S12' and self.__getState(recordList[i].getState())=='S1':
                        time = recordList[i+1].getTime() 

            else:
                pass
        return int(time)

    def __stateDetect(self, recordList, event):
        stateNameList = ['釜门开','釜门关','开始预养','从邻釜导气','从隔釜导气','升压','恒压','给邻釜预养','导气到邻釜','导气到隔釜','降压','排空']
        startTime = event.getStartTime()
        endTime = event.getEndTime()
        if(endTime == 0):
            endTime = 1000000000000000

        index = 0
        for i in range(1,len(recordList)):
            autoClaveData = recordList[i]
            rectime = int(autoClaveData.getTime())
            if rectime >= startTime and rectime <= endTime:
                if self.__getState(recordList[i-1].getState())=='S12' and self.__getState(recordList[i].getState())=='S1':
                    recDict = {'name':stateNameList[0], 'time':rectime, 'index': index}
                    event.pushStateTime(recDict)              

                for si in range (1, 12):
                    if self.__getState(recordList[i-1].getState())=='S'+str(si) and self.__getState(recordList[i].getState())=='S'+str(si+1):
                        recDict = {'name':stateNameList[si], 'time':rectime, 'index': index}
                        event.pushStateTime(recDict)


                index = index+1

        return event
    

    def __getState(self, val):
        itv = 340
        if val > 0*itv and val <= 1*itv:
            return 'S1'
        elif val > 1*itv and val <= 2*itv:
            return 'S2'
        elif val > 2*itv and val <= 3*itv:
            return 'S3'
        elif val > 3*itv and val <= 4*itv:
            return 'S4'
        elif val > 4*itv and val <= 5*itv:
            return 'S5'
        elif val > 5*itv and val <= 6*itv:
            return 'S6'
        elif val > 6*itv and val <= 7*itv:
            return 'S7'
        elif val > 7*itv and val <= 8*itv:
            return 'S8'
        elif val > 8*itv and val <= 9*itv:
            return 'S9'
        elif val > 9*itv and val <= 10*itv:
            return 'S10'
        elif val > 10*itv and val <= 11*itv:
            return 'S11'
        elif val > 11*itv and val <= 4095:
            return 'S12'
