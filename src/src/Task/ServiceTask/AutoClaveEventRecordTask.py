import abc
import json
import os
import sys
import schedule
import time


from src.Factory import DISDataToolFactory
from src.Factory import OBSDataToolFactory
from src.DataSet import AutoClaveRecordDataSet
from src.Data.Data import AutoClaveData
from model.Task import Task


#任务抽象类
class AutoClaveEventRecordTask(Task):

    def __init__(self, nowTime, dataSet, claveNum, obsDataTool):
        self.nowTime = nowTime
        self.dataSet = dataSet
        self.claveNum = claveNum
        self.obsDataTool = obsDataTool

  
    def getType(self):
        return "AutoClaveEventRecordTask"

    
    def __eventDetect(self, claveId, lastTime):
        pass

   
    def run(self):
        autoClaveRecord = AutoClaveRecordDataSet(self.claveNum, self.nowTime)
        autoClaveRecord = self.obsDataTool.getData(autoClaveRecord)
        

        pass

