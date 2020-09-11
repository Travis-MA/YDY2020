import abc
import json
import os
import sys
import schedule
import time


from src.Factory import DISDataToolFactory
from src.Factory import OBSDataToolFactory
from src.DataSet import AutoClaveRealTimeDataSet
from src.Data.Data import AutoClaveData
from model.Task import ScheduleTask


#任务抽象类
class DIStoOBSscheduleTask(ScheduleTask):
    __period = 0

    def __init__(self, period):
        self.__period = period


    def __job(self):


        #从DIS采集蒸压釜实时数据，并上传到OBS
        claveNum = 7
        disDataTool = DISDataToolFactory().newObject('shardId-0000000000', 0, 'dis-YDY1')
        dataSet = AutoClaveRealTimeDataSet(claveNum)
        dataSet = disDataTool.getData(dataSet)

        obsDataTool = OBSDataToolFactory().newObject('obs-ydy1')
        obsDataTool.postData(dataSet)


    def setPeriod(self, period):
        self.__period = period


  
    def getType(self):
        return "DIStoOBSscheduleTask"

   
    def run(self,para):

        if para == 1:
            self.__job()
        else:
            schedule.every(self.__period).minutes.do(self.__job)
        
        pass

