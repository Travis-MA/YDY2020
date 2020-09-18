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
        todayFolderPath = folderPath+offSetTime.date().isoformat()+'/'

        for claveId in range (1,claveNum+1):
            singleClaveEventList = self.dataObj.getSet(claveId).getSet()
            for event in singleClaveEventList:
                if(event.getSet('json') != 0):
                    self.OBSTool.writeContent(prefix = todayFolderPath+event.getPrefix(), metaData = str(event.getSet('json'))) #新建今日文件夹

                    
                


