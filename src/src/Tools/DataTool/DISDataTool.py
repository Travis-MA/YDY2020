#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import json
import os
import sys

from model.Tools import DataTool
from configparser import ConfigParser

from src.Algorithm.AutoClaveAlgorithm.ACGetDataDIS import ACGetDataDIS

#DIS数据工具
class DISDataTool(DataTool):

    __confPath = ''
    __streamName = ''
    __partitionId = ''
    __startSeq = 0

    def __init__(self, cli):
        self.cli = cli
        self.__confPath = ''
        self.__streamName = ''
        self.__partitionId = ''
        self.__startSeq = ''
        pass

    # enter getCursor endpoint information
    def getCursor_test(self):
        try:
            r = self.cli.getCursor(streamName=self.__streamName,partitionId=self.__partitionId,cursorType='TRIM_HORIZON',startSeq=self.__startSeq)
            return r.cursor
        except Exception as ex:
            print('DISDataMan __getCursor_test '+str(ex))
    # Download data
    def getRecords_test(self):
        cursor=self.getCursor_test()
        #print('cursor '+cursor)
        records = []
        try:
            while cursor:
                r = self.cli.getRecords(partitioncursor=cursor)
                cursor = r.nextPartitionCursor
                if r.recordResult == []:
                    break

                #print(r.statusCode)
                # print(r.recordResult)
                # print(r.body)
                # print(r.body["records"])
                records.extend(r.body["records"])
                

                # for i in r.getRecordResult(r.recordResult):
                #     print("%s %s" % (i.sequence_number, i.data))
            return records
        except Exception as ex:
            print('DISDataMan __getRecords_test: '+str(ex))


    def getType(self):
        return 'DISDataTool'

    def getData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':          
            return ACGetDataDIS(self,dataObj).run()
        else:
            pass

    def postData(self, dataObj):
        pass

    def setConfPath(self, val):
        self.__confPath = val
    
    def getConfPath(self):
        return self.__confPath

    def setStreamName(self, val):
        self.__streamName = val
 
    def setPartitionId(self, val):
        self.__partitionId = val

    def setStartSeq(self, val):
        self.__startSeq = val

           
