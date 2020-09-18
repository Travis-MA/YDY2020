# -*- coding:utf-8 -*-
from model.Algorithm import Algorithm
from datetime import timedelta
from datetime import datetime
import json

folderPath = 'Service/ZyRecord/'

class ACRecordOBS:

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

        for claveId in range (1,claveNum+1):
            singleClaveEventList = self.dataObj.getSet(claveId)
            


    def newRecord(self, claveId, todayInitial, offSetTime):
        recordDict = {
            "FuId": claveId,
            "startTime":int(todayInitial.timestamp())*1000,
            "endTime":0,
            "stateTime":[],
            "data":{"pressure": [], "tempIn": [], "tempOut": [], "state":[]}
        }
        todayFolderPath = folderPath+offSetTime.date().isoformat()+'/'
        eventPrefix = todayFolderPath+str(claveId)+'XING'+str(int(todayInitial.timestamp())*1000)+'Y'
        self.dataObj.getSet(claveId).pushData(eventPrefix)
        recordJson = json.dumps(recordDict)
        self.OBSTool.writeContent(prefix = folderPath+offSetTime.date().isoformat()+'/'+eventPrefix, metaData = str(recordJson)) #新建今日文件夹
