#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
from src.Tools import DISDataTool

class DISDataToolFactory:

    def __init__(self):
        pass

    def newObject(self, partitionId, startSeq, streamName):
        
        print('[DISDataToolFactory] newObject')
        newDisDataTool = DISDataTool()

        newDisDataTool.setConfPath('conf.ini')
        newDisDataTool.setStreamName(streamName)
        newDisDataTool.setStartSeq(startSeq)
        newDisDataTool.setPartitionId(partitionId)
    
        return newDisDataTool
    
    def getType(self):
        return 'DISDataToolFactory'


