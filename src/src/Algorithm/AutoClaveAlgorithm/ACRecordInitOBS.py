# -*- coding:utf-8 -*-
from model.Algorithm import Algorithm
from datetime import timedelta
from datetime import datetime
import json

folderPath = 'Service/ZyRecord/'

class ACRecordInitOBS:

    def __init__(self, OBSTool, dataObj):
        self.OBSTool = OBSTool
        self.dataObj = dataObj

    def getType(self):
        return "ACRecordOBS"

    def run(self):
        nowTime = self.dataObj.getNowTime() #真实时间
        claveNum = self.dataObj.getClaveNum()
        offSetTime = nowTime-nowTime.tzinfo.utcoffset(nowTime) #减去7个小时的时间（今天上午七点前是昨天）
        offSetHour = int(nowTime.tzinfo.utcoffset(nowTime).total_seconds()/3600)
        todayInitial = datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day,hour=offSetHour,minute=0,second=0)
        todayFolderPath = folderPath+offSetTime.date().isoformat()+'/'
        todayRecordList = self.OBSTool.listObject(todayFolderPath)
        if(len(todayRecordList)>0): #有今天的
            for claveId in range(1, claveNum+1):
                for content in todayRecordList:
                    Xindex = content.key.find("X")
                    if(content.key[Xindex-1:Xindex]==str(claveId)):
                        self.dataObj.pushData(claveId, content.key)

            print('hasToday')
        else: #没有今天的
            print('noToday')


            offSetTimeYestd = offSetTime-timedelta(days=1)
            yestdFolderPath = folderPath+offSetTimeYestd.date().isoformat()+'/'
            yestdRecordList = self.OBSTool.listObject(yestdFolderPath)
            if(len(yestdRecordList)>0): #有昨天的
                for claveId in range(1,claveNum+1):
                    flag = 0
                    for content in yestdRecordList:
                        Xindex = content.key.find("X")
                        if(content.key[Xindex-1:Xindex]==str(claveId)):
                            if content.key[Xindex+1:Xindex+4] == 'ING':
                                newTodayKey = todayFolderPath+content.key[Xindex-1:]
                                self.OBSTool.copyObject(content.key, newTodayKey)
                                self.dataObj.pushData(claveId, newTodayKey)
                                flag = 1
                    if(flag == 0):
                        self.newRecord(claveId, todayInitial, offSetTime)

            else: #没有昨天的
                for claveId in range(1,claveNum+1):
                    self.newRecord(claveId, todayInitial, offSetTime)
                
        return self.dataObj   

        #查找是否有今天的文件夹，如果没有就新建，并且去昨天的找，如果昨天也没有，那么就从7点算起建立新事件

    def backSearch(self, data):

        timeStemp = 0

        return timeStemp

    def newRecord(self, claveId, todayInitial, offSetTime):
        recordDict = {
            "FuId": claveId,
            "startTime":int(todayInitial.timestamp())*1000,
            "endTime":0,
            "data":{"pressure": [], "tempIn": [], "tempOut": [], "state":[]}
        }
        todayFolderPath = folderPath+offSetTime.date().isoformat()+'/'
        eventPrefix = todayFolderPath+str(claveId)+'XING'+str(int(todayInitial.timestamp())*1000)+'Y'
        self.dataObj.pushData(claveId,eventPrefix)
        recordJson = json.dumps(recordDict)
        self.OBSTool.writeContent(prefix = folderPath+offSetTime.date().isoformat()+'/'+eventPrefix, metaData = str(recordJson)) #新建今日文件夹
