#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
import sys
from model.Tools import DataTool
sys.path.append('..\\lib')
from obs import ObsClient, Object, DeleteObjectsRequest
from configparser import ConfigParser

#OBS数据人
class OBSDataTool(DataTool):

    __confPath = ''
    __bucketName = ''

    def __init__(self):
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
                obsRecDict = {'claveId':claveId, 'lastTime':record.getLastTime(), 'para':recordList}
                obsRecPrefix = 'ZyRealTime/clave'+str(claveId)
                self.__writeStr(obsRecPrefix, obsRecDict)


    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def __writeStr(self, prefix, str):
        conf = ConfigParser()
        conf.read(self.__confPath)
        # Use configuration file
        try:          
            ak = conf.get('OBSconfig','ak')
            sk = conf.get('OBSconfig','sk')
            server = conf.get('OBSconfig','server')
        
            obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=server)
            resp = obsClient.listObjects(self.__bucketName, folderPrefix)
            for content in resp.body.contents:
                print('\t' + content.key + ' etag[' + content.etag + ']')

        except Exception as ex:
            print('OBSDataTool __listObjectInFolder: ' + str(ex))
        pass

    def __listObjectsInFolder(self, folderPrefix):
        conf = ConfigParser()
        conf.read(self.__confPath)
        # Use configuration file
        try:          
            ak = conf.get('OBSconfig','ak')
            sk = conf.get('OBSconfig','sk')
            server = conf.get('OBSconfig','server')
        
            obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=server)
            resp = obsClient.listObjects(self.__bucketName, folderPrefix)
            for content in resp.body.contents:
                print('\t' + content.key + ' etag[' + content.etag + ']')

        except Exception as ex:
            print('OBSDataTool __listObjectInFolder: ' + str(ex))
        pass
   



