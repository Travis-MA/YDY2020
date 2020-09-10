#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
from src.Tools import OBSDataTool


class OBSDataToolFactory:

    def __init__(self):
        pass

    def newObject(self, bucketName):
        
        print('[OBSDataToolFactory] newObject')
        newObsDataTool = OBSDataTool()

        newObsDataTool.setConfPath('conf.ini')
        newObsDataTool.setBucketName(bucketName)

    
        return newObsDataTool
    
    def getType(self):
        return 'ObsDataToolFactory'


