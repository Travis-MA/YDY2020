#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import Data
import Tools

#工厂抽象类 包括两种子类类型（任务工厂， 工具工厂）
class Factory(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def getType(self):
        pass

    @abc.abstractmethod 
    def newObject(self):
        pass


#工具工厂抽象类 
class ToolsFactory(Factory, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def name(self, name):
        pass

#生产DIS数据人的工具工厂
class DISDataToolFactory():
    

    def name(self):
        return 'DISDataToolFactory'

    def newObject(self, partitionId, startSeq, streamName):
        
        print('FROM DISDataToolFactory: newObject')
        newDisDataTool = Tools.DISDataTool()

        newDisDataTool.setConfPath('conf.ini')
        newDisDataTool.setStreamName(streamName)
        newDisDataTool.setStartSeq(startSeq)
        newDisDataTool.setPartitionId(partitionId)
    
        return newDisDataTool
    
    def getType(self):
        return 'DISDataToolFactory'

#任务工厂抽象类 
class TaskFactory(Factory, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def name(self, name):
        pass


