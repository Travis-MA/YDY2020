import abc
import time
import json
import os
import sys
import schedule
from datetime import datetime
from datetime import timezone
from datetime import timedelta


from src.Factory import DISDataToolFactory
from src.Factory import OBSDataToolFactory
from src.DataSet import AutoClaveRealTimeDataSet
from src.DataSet import AutoClaveRecordDataSet
from model.Task import ScheduleTask
from model.Task import Task
from src.Algorithm.AutoClaveAlgorithm.ACTimeDomainAnalysisOBS import ACTimeDomainAnalysisOBS

#任务抽象类
class DIStoOBSscheduleTask(ScheduleTask):
    __period = 0

    def __init__(self, period):
        self.__period = period


    def __job(self):


        #从DIS采集蒸压釜实时数据，并上传到OBS
        claveNum = 7
        disDataTool = DISDataToolFactory().newObject('shardId-0000000000', 0, 'dis-YDY1')
        #dataSet = 0
        dataSet = AutoClaveRealTimeDataSet(claveNum)
        dataSet = disDataTool.getData(dataSet)

        obsDataTool = OBSDataToolFactory().newObject('obs-ydy1')
        obsDataTool.postData(dataSet)

        hourOffset = 7
        today = datetime.today()
        nowTime = datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, minute=today.minute, second=today.second, microsecond=0, tzinfo=timezone(timedelta(hours=hourOffset)), fold=0)
        autoClaveRecordTask = AutoClaveEventRecordTask(nowTime, dataSet, claveNum, obsDataTool)
        autoClaveRecordTask.run()

    def setPeriod(self, period):
        self.__period = period


  
    def getType(self):
        return "DIStoOBSscheduleTask"

   
    def run(self,para):

        if para == 1:
            self.__job()
        else:
            schedule.every(self.__period).minutes.do(self.__job)
            while True:
                schedule.run_pending()
                time.sleep(1)
        
        


#任务抽象类
class AutoClaveEventRecordTask(Task):

    def __init__(self, nowTime, dataSet, claveNum, obsDataTool):
        self.nowTime = nowTime
        self.dataSet = dataSet
        self.claveNum = claveNum
        self.obsDataTool = obsDataTool

  
    def getType(self):
        return "AutoClaveEventRecordTask"

   
    def run(self):
        maxIter = 10
        iter = 0
        ifAllEnd = 0
        while ifAllEnd == 0 and iter < maxIter:
            oldAutoClaveRecord = AutoClaveRecordDataSet(self.claveNum, self.nowTime)
            oldAutoClaveRecord = self.obsDataTool.getData(oldAutoClaveRecord)
            newAutoClaveRecord, ifAllEnd = ACTimeDomainAnalysisOBS(self, oldAutoClaveRecord, self.dataSet).run()
            self.obsDataTool.postData(newAutoClaveRecord)
            time.sleep(1)
        


