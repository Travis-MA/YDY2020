# -*- coding:utf-8 -*-
from model.Algorithm import Algorithm

class ACRecordOBS:

    def __init__(self, OBSTool, dataObj):
        self.OBSTool = OBSTool
        self.dataObj = dataObj

    def getType(self):
        return "ACRecordOBS"

    def run(self):
        nowTime = self.dataObj.getNowTime() #真实时间
        offSetTime = nowTime-nowTime.tzinfo.utcoffset(nowTime) #减去7个小时的时间（今天上午七点前是昨天）
        todayRecordList = self.OBSTool.listObject('Service/ZyRecord/'+offSetTime.date().isoformat()+'/')
        if(len(todayRecordList)>0): #有今天的
            pass
        else: #没有今天的
            pass

        #查找是否有今天的文件夹，如果没有就新建，并且去昨天的找，如果昨天也没有，那么就从7点算起建立新事件

