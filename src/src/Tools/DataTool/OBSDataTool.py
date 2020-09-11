#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
import sys
from model.Tools import DataTool
sys.path.append('..\\lib')
from obs import ObsClient, Object, DeleteObjectsRequest, PutObjectHeader
from configparser import ConfigParser

#OBS数据人
class OBSDataTool(DataTool):

    __confPath = ''
    __bucketName = ''

    def __init__(self, obsClient):
        self.__obsClient = obsClient
        self.__confPath = ''
        self.__bucketName = ''
        pass

    def getType(self):
        return "OBSDataTool"

#传递一个data过去？？？？？
    def getData(self, dataObj):
        pass
    
    def postData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            for claveId in range(1,dataObj.getClaveNum()+1):
                record = dataObj.getSet(claveId)
                recordList = []
                for data in record.getSet():
                    recDict = {'time':data.getTime(), 'inTemp':data.getInTemp(), 'outTemp':data.getOutTemp(), 'inPress':data.getInPress(), 'state':data.getState()}
                    recordList.append(recDict)
                obsRecDict = {'claveId':claveId, 'lastTime':record.getLastTime(), 'records':recordList}
                obsRecPrefix = 'Service/ZyRealTime/clave'+str(claveId)
                print('OBS write len:'+str(len(recordList))+' devId:'+dataObj.getDevId(claveId))
                self.__deleteObject(obsRecPrefix)
                self.__writeContent(obsRecPrefix, json.dumps(obsRecDict))


    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def __deleteObject(self, prefix):
        resp = self.__obsClient.deleteObject(self.__bucketName, prefix)
        if resp.status < 300:
            print('Delete object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)
       

    def __writeContent(self, prefix, metaData):

        resp = self.__obsClient.putContent(self.__bucketName, prefix, str(metaData))
        if resp.status < 300:
            print('Create object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)


    def __listObjectsInFolder(self, folderPrefix):
        
        resp = self.__obsClient.listObjects(self.__bucketName, folderPrefix)
        for content in resp.body.contents:
            print('\t' + content.key + ' etag[' + content.etag + ']')

   



