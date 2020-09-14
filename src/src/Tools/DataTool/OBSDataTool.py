#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
import sys
from model.Tools import DataTool
sys.path.append('..\\lib')
from obs import ObsClient, Object, DeleteObjectsRequest, PutObjectHeader
from configparser import ConfigParser

from src.Algorithm.AutoClaveAlgorithm.ACRealTimeOBS import ACRealTimeOBS
from src.Algorithm.AutoClaveAlgorithm.ACRecordOBS import ACRecordOBS


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
        if dataObj.getType() == 'AutoClaveRecordDataSet':
            ACRecordOBS(self, dataObj).run()
  
        else:
            pass
    
    def postData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            ACRealTimeOBS(self,dataObj).run()


    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def deleteObject(self, prefix):
        resp = self.__obsClient.deleteObject(self.__bucketName, prefix)
        if resp.status < 300:
            print('Delete object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)

    def listObject(self, prefix):
        # 调用listObjects接口列举指定桶内的所有对象
        resp = self.__obsClient.listObjects(self.__bucketName, prefix=prefix)
        if resp.status < 300: 
            # 输出请求Id   
            print('requestId:', resp.requestId)
            print(prefix)
            index = 0
            # 遍历输出所有对象信息
            objList = []
            for content in resp.body.contents:
                objList.append(content)
                # 输出该对象名content.key
                # 输出该对象的最后修改时间content.lastModified
                # 输出该对象大小content.size
                index += 1
            return objList
        else:  
            # 输出错误码  
            print('errorCode:', resp.errorCode)
            # 输出错误信息
            print('errorMessage:', resp.errorMessage)

       

    def writeContent(self, prefix, metaData):

        resp = self.__obsClient.putContent(self.__bucketName, prefix, str(metaData))
        if resp.status < 300:
            print('Create object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)


    def listObjectsInFolder(self, folderPrefix):
        
        resp = self.__obsClient.listObjects(self.__bucketName, folderPrefix)
        for content in resp.body.contents:
            print('\t' + content.key + ' etag[' + content.etag + ']')

   



