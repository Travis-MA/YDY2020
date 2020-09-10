#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import json
import os
import sys

from model.Tools import DataTool
import src.Data.Data as Data
sys.path.append('..\\lib')
from DIS.client import disclient
from configparser import ConfigParser

#DIS数据工具
class DISDataTool(DataTool):
    
    print(sys.path)


    __confPath = ''
    __streamName = ''
    __partitionId = ''
    __startSeq = 0

    def __test(self):
        conf = ConfigParser()
        conf.read(self.__confPath)
        # Use configuration file
        try:
            projectid = conf.get('DISconfig','projectid')
            ak = conf.get('DISconfig','ak')
            sk = conf.get('DISconfig','sk')
            region = conf.get('DISconfig','region')
            endpoint = conf.get('DISconfig','endpoint')

            try:
                return disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
            except Exception as ex:
                print('DISDataMan __test2: ' + str(ex))

        except Exception as ex:
            print('DISDataMan __test1: ' + str(ex))
        pass
    # enter getCursor endpoint information
    def __getCursor_test(self):
        try:
            cli=self.__test()
            r = cli.getCursor(streamName=self.__streamName,partitionId=self.__partitionId,cursorType='TRIM_HORIZON',startSeq=self.__startSeq)
            return r.cursor
        except Exception as ex:
            print('DISDataMan __getCursor_test '+str(ex))
    # Download data
    def __getRecords_test(self):
        cli=self.__test()
        cursor=self.__getCursor_test()
        print('cursor '+cursor)
        records = []
        try:
            while cursor:
                r = cli.getRecords(partitioncursor=cursor)
                cursor = r.nextPartitionCursor
                if r.recordResult == []:
                    break

                print(r.statusCode)
                # print(r.recordResult)
                # print(r.body)
                # print(r.body["records"])
                records.extend(r.body["records"])
                

                # for i in r.getRecordResult(r.recordResult):
                #     print("%s %s" % (i.sequence_number, i.data))
            return records
        except Exception as ex:
            print('DISDataMan __getRecords_test: '+str(ex))

    def __init__(self):
        pass

    def getType(self):
        return 'DISDataTool'

    def getData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            sourceData = self.__getRecords_test()
            #print(sourceData)
            for devRec in sourceData:
                data = json.loads(devRec['data'])
                services = data['services'][0]
                properties = services['properties']
                dev_id = data['device_id']
                for claveId in range(1, dataObj.getClaveNum()+1):
                    if dev_id == dataObj.getDevId(claveId):
                        conf = ConfigParser()
                        conf.read(self.__confPath)
                        # Use configuration file
                        try:
                            inTempChannel = conf.get('AutoClave'+str(claveId),'inTempChannel')
                            outTempChannel = conf.get('AutoClave'+str(claveId),'outTempChannel')
                            inPressChannel = conf.get('AutoClave'+str(claveId),'inPressChannel')
                            stateChannel = conf.get('AutoClave'+str(claveId),'stateChannel')

                            recData =  Data.AutoClaveData(claveId)
                            recData.setTime(services['event_time'])
                            recData.setInTemp(float(properties[inTempChannel]))
                            recData.setOutTemp(float(properties[outTempChannel]))
                            recData.setInPress(float(properties[inPressChannel]))
                            recData.setState(float(properties[stateChannel]))
                        
                            dataObj.pushData(claveId, recData)
                        except Exception as ex:
                            print('[DISDataTool](getData)' + str(ex))
                        
            return dataObj
        else:
            pass

    def postData(self, dataObj):
        pass

    def setConfPath(self, val):
        self.__confPath = val

    def setStreamName(self, val):
        self.__streamName = val
 
    def setPartitionId(self, val):
        self.__partitionId = val

    def setStartSeq(self, val):
        self.__startSeq = val

           
