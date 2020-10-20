#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
import sys
from model.Tools import DataTool
sys.path.append('..//lib') #linux
#sys.path.append('..\\lib') #win
from obs import ObsClient, Object, DeleteObjectsRequest, PutObjectHeader
from configparser import ConfigParser

from src.Algorithm.AutoClaveAlgorithm.ACRealTimeOBS import ACRealTimeOBS
from src.Algorithm.AutoClaveAlgorithm.ACRecordOBS import ACRecordOBS
from src.Algorithm.AutoClaveAlgorithm.ACRecordInitOBS import ACRecordInitOBS


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

    def getData(self, dataObj):
        #返回一个包含当日事件列表的对象，并包含数据指针，如果没有当日，会做当日初始化
        if dataObj.getType() == 'AutoClaveRecordDataSet':
            dataObj = ACRecordInitOBS(self, dataObj).run()
            return dataObj

        elif dataObj.getType() == 'AutoClaveRealTimeDataSet':
            pass
    
    def postData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            ACRealTimeOBS(self,dataObj).run()
        elif dataObj.getType() == 'AutoClaveRecordDataSet':
            ACRecordOBS(self,dataObj).run()



    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def deleteObject(self, prefix):
        resp = self.__obsClient.deleteObject(self.__bucketName, prefix)
        if resp.status >= 300:
            print('OBS DELETE obj: common msg:status:', resp.status, 'prefix ', prefix, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)

    def copyObject(self, fromPrefix, toPrefix):
        resp = self.__obsClient.copyObject(self.__bucketName, fromPrefix, self.__bucketName, toPrefix)

        if resp.status >= 300:
            print('OBS copy obj:errorCode:', resp.errorCode)
            print('OBS copy obj:errorMessage:', resp.errorMessage)

    def listObject(self, prefix):
        # 调用listObjects接口列举指定桶内的所有对象
        resp = self.__obsClient.listObjects(self.__bucketName, prefix=prefix)
        if resp.status < 300:
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
            print('OBS listobj:errorCode:', resp.errorCode)
            # 输出错误信息
            print('OBS listobj:errorMessage:', resp.errorMessage)


    def writeContent(self, prefix, metaData):

        resp = self.__obsClient.putContent(self.__bucketName, prefix, metaData)
        if resp.status >= 300:
            print('OBS create obj:common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)






