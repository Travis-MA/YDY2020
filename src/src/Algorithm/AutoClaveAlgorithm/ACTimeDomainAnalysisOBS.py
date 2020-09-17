# -*- coding:utf-8 -*-
import json
import numpy as np
import src.Data.Data as Data
from model.Algorithm import Algorithm
from configparser import ConfigParser
from datetime import datetime
from datetime import timezone
from datetime import timedelta

class ACTimeDomainAnalysisOBS(Algorithm):

    def __init__(self, OBSTool, oldRecord, realTimeRecord):

        self.OBSTool = OBSTool
        self.oldRecord = oldRecord
        self.realTimeRecord = realTimeRecord

    
    def getType(self):
        return "ACTimeDomainAnalysisOBS"

    def run(self):
        for claveId in range(1,self.realTimeRecord.getClaveNum()+1):
            print('for clave:'+str(claveId))
            startTime = 0
            #找到最新纪录时间
            for event in self.oldRecord.getSet(claveId).getSet():
                prefix = event['eventPrefix']
                Xindex = prefix.find("X")
                Yindex = prefix.find("Y")
                if(prefix[Xindex+1:Xindex+4] == "ING"):
                    time = int(prefix[Xindex+4:Yindex])
                    startTime = time
                elif(prefix[Xindex+1:Xindex+4] == "FIN"): 
                    time = int(prefix[Yindex:])
                    if time>startTime:
                        startTime = time

            startTime = int(startTime/1000)
            #得到实时数据
            timeList = []
            inTempList =  []
            outTempList = []
            inPressList = []
            stateList = []
            recordList = self.realTimeRecord.getSet(claveId).getSet('list')
            for autoClaveData in recordList:
                time = int(datetime.strptime(autoClaveData.getTime(),'%Y%m%dT%H%M%SZ').timestamp())
                #print('time:'+str(time)+" stat:"+str(startTime))
                if(time>startTime):
                    inTemp = int(autoClaveData.getInTemp())
                    outTemp = int(autoClaveData.getOutTemp())
                    inPress = int(autoClaveData.getInPress())
                    state = int(autoClaveData.getState())

                    timeList.append(time)
                    inTempList.append(inTemp)
                    outTempList.append(outTemp)
                    inPressList.append(inPress)
                    stateList.append(state)


                    #print("Id:"+str(claveId)+" T:"+str(time)+" iT:"+str(inTemp)+" oT:"+str(outTemp)+" iP:"+str(inPress)+" S:"+str(state))

            dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList])
            print(dataSet)
    