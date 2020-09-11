#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import sys
from src.Tools import OBSDataTool
sys.path.append('..\\lib')
from obs import ObsClient, Object, DeleteObjectsRequest
from configparser import ConfigParser
confPath = 'conf.ini'


class OBSDataToolFactory:

    def __init__(self):
        pass

    def newObject(self, bucketName):
        conf = ConfigParser()
        conf.read(confPath)
        # Use configuration file
        try:          
            ak = conf.get('OBSconfig','ak')
            sk = conf.get('OBSconfig','sk')
            server = conf.get('OBSconfig','server')
            obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=server)
            
            newObsDataTool = OBSDataTool(obsClient)
            newObsDataTool.setConfPath(confPath)
            newObsDataTool.setBucketName(bucketName)
            return newObsDataTool

        except Exception as ex:
            print('New OBS object ' + str(ex))
        pass
    

    
    def getType(self):
        return 'ObsDataToolFactory'


