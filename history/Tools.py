#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import json
import os
import sys

#工具抽象类
class Tools(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def getType(self):
        pass


#数据工具
class DataTool(Tools, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setConfPath(self):
        pass

    @abc.abstractmethod
    def getData(self):
        pass

    @abc.abstractmethod
    def postData(self, data):
        pass


#DIS数据工具
class DISDataTool(DataTool):
    
    from src.com.dis.client import disclient
    from configparser import ConfigParser

    __confPath = ''
    __streamName = ''
    __partitionId = ''
    __startSeq = 0

    def __test(self):
        conf = self.ConfigParser()
        conf.read(self.__confPath)
        # Use configuration file
        try:
            projectid = conf.get('DISconfig','projectid')
            ak = conf.get('DISconfig','ak')
            sk = conf.get('DISconfig','sk')
            region = conf.get('DISconfig','region')
            endpoint = conf.get('DISconfig','endpoint')

            try:
                return self.disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
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
                    print('empty')
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


    def getType(self):
        return 'DISDataTool'

    def getData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeData':
            dataObj.setData(self.__getRecords_test())
            return dataObj
        else:
            pass

    def postData(self, data):
        pass

    def setConfPath(self, val):
        self.__confPath = val

    def setStreamName(self, val):
        self.__streamName = val
 
    def setPartitionId(self, val):
        self.__partitionId = val

    def setStartSeq(self, val):
        self.__startSeq = val

           

#OBS数据人
class OBSDataTool(DataTool):

    from src.obs import ObsClient, Object, DeleteObjectsRequest
    from configparser import ConfigParser
    __confPath = ''
    __bucketName = ''

    def name(self):
        return "OBSDataMan"

#传递一个data过去？？？？？
    def getData(self):
        pass
    
    def postData(self, data):
        pass

    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def listObjectsInFolder(self, folderPrefix):
        conf = self.ConfigParser()
        conf.read(self.__confPath)
        # Use configuration file
        try:          
            ak = conf.get('OBSconfig','ak')
            sk = conf.get('OBSconfig','sk')
            server = conf.get('OBSconfig','server')
        
            obsClient = self.ObsClient(access_key_id=ak, secret_access_key=sk, server=server)
            resp = obsClient.listObjects(self.__bucketName, folderPrefix)
            for content in resp.body.contents:
                print('\t' + content.key + ' etag[' + content.etag + ']')

        except Exception as ex:
            print('OBSDataMan __listObjectInFolder: ' + str(ex))
        pass
   



