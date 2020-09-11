#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import sys
from src.Tools import DISDataTool
sys.path.append('..\\lib')
from DIS.client import disclient
from configparser import ConfigParser
confPath = 'conf.ini'

class DISDataToolFactory:

    def __init__(self):
        pass

    def newObject(self, partitionId, startSeq, streamName):
        conf = ConfigParser()
        conf.read(confPath)
        # Use configuration file
        try:
            projectid = conf.get('DISconfig','projectid')
            ak = conf.get('DISconfig','ak')
            sk = conf.get('DISconfig','sk')
            region = conf.get('DISconfig','region')
            endpoint = conf.get('DISconfig','endpoint')

            try:
                dis = disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
                newDisDataTool = DISDataTool(dis)
                newDisDataTool.setConfPath(confPath)
                newDisDataTool.setStreamName(streamName)
                newDisDataTool.setStartSeq(startSeq)
                newDisDataTool.setPartitionId(partitionId)
                return newDisDataTool

            except Exception as ex:
                print('[DISDataToolFactory] NewDis' + str(ex))

        except Exception as ex:
            print('[DISDataToolFactory] conf load ' + str(ex))
        pass
        

    

    
    def getType(self):
        return 'DISDataToolFactory'


