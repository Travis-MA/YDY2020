import abc

class Algorithm(metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getType(self):
        pass

    @abc.abstractmethod
    def run(self, para):
        pass

import abc

#数据抽象类
class Data(metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getType(self):
        pass

import abc

#数据抽象类 包括三种子类类型（实时数据RealTime， 记录Record， 目录List）
class DataSet(metaclass = abc.ABCMeta):



    @abc.abstractmethod
    def getType(self):
        pass

    @abc.abstractmethod
    def pushData(self,data):
        pass

    @abc.abstractmethod
    def getSet(self):
        pass

#工厂抽象类 包括两种子类类型（任务工厂， 工具工厂）
class Factory(metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getType(self):
        pass

    @abc.abstractmethod
    def newObject(self):
        pass


#工具工厂抽象类
class ToolsFactory(Factory, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def name(self, name):
        pass


#任务工厂抽象类
class TaskFactory(Factory, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def name(self, name):
        pass


import abc

#任务抽象类
class Task(metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getType(self):
        pass

    @abc.abstractmethod
    def run(self, para):
        pass


#定时任务抽象类
class ScheduleTask(Task, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setPeriod(self, period):
        pass

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

class SingleAutoClaveRecordEvent(DataSet):
    __prefix = ''
    __recordList = []
    __stateTime = []
    __startTime = 0
    __endTime = 0

    def __init__(self, prefix, claveId):
        self.__prefix = prefix
        self.__recordList = []
        self.__stateTime = []
        self.__claveId = claveId
        self.__startTime = 0
        self.__endTime = 0

    def getClaveId(self):
        return self.__claveId

    def getPrefix(self):
        return self.__prefix

    def setPrefix(self, prefix):
        self.__prefix = prefix

    def getType(self):
        return "SingleAutoClaveRecordEvent"

    def pushData(self, data):
        self.__recordList.append(data)

    def setStartTime(self, startTime):
        self.__startTime = startTime

    def getStartTime(self):
        return self.__startTime

    def setEndTime(self, endTime):
        self.__endTime = endTime

    def getEndTime(self):
        return self.__endTime

    def pushStateTime(self, stateTime):
        self.__stateTime.append(stateTime)

    def getSet(self, para):
        if para == 'json':
            if (len(self.__recordList) <= 1):
                return 0
            else:
                pressure = []
                tempIn = []
                tempOut = []
                state = []
                for data in self.__recordList:
                    time = data.getTime()

                    tempIn.append({'t': time, 'v': data.getInTemp()})
                    tempOut.append({'t': time, 'v': data.getOutTemp()})
                    pressure.append({'t': time, 'v': data.getInPress()})
                    state.append({'t': time, 'v': data.getState('val')})

                recordDict = {'FuId': self.__claveId, 'startTime': self.__startTime, 'endTime': self.__endTime,
                              'stateTime': self.__stateTime,
                              'data': {'pressure': pressure, 'tempIn': tempIn, 'tempOut': tempOut, 'state': state}}
                recordJson = json.dumps(recordDict)
                return recordJson
        else:
            return self.__recordList


class SingleAutoClaveRecordDataSet(DataSet):

    def __init__(self, claveId):
        self.eventList = []
        self.claveId = claveId

    def getType(self):
        return "SingleAutoClaveRecordDataSet"

    def pushData(self, data):
        if (isinstance(data, str)):
            event = SingleAutoClaveRecordEvent(data, self.claveId)
            self.eventList.append(event)

        else:
            self.eventList.append(data)

    def getSet(self):
        return self.eventList


# 蒸压釜数据记录
class AutoClaveRecordDataSet(DataSet):

    def __init__(self, claveNum, nowTime):
        self.subRecordSetList = []
        self.nowTime = nowTime
        self.claveNum = claveNum
        for claveId in range(1, self.claveNum + 1):
            subRecordSet = SingleAutoClaveRecordDataSet(claveId)
            self.subRecordSetList.append(subRecordSet)

    def getRecordDate(self):
        return self.recordDate

    def getType(self):
        return "AutoClaveRecordDataSet"

    def getNowTime(self):
        return self.nowTime

    def getClaveNum(self):
        return self.claveNum

    def newEvent(self, prefix, claveId):
        return SingleAutoClaveRecordEvent(prefix, claveId)

    def pushData(self, claveId, data):
        self.subRecordSetList[claveId - 1].pushData(data)

    def getSet(self, claveId):
        return self.subRecordSetList[claveId - 1]


import numpy as np
from src.model.DataSet import DataSet

print('okok')

dev_prefix = '5f61ea5045765502bcee8ab6_0'


class SingleAutoClaveRealtimeDataSet(DataSet):
    __recordList = []
    __claveID = 0
    __device_id = ''

    def __init__(self, ID, dev_ID):
        self.__claveID = ID
        self.__device_id = dev_ID
        self.__recordList = []
        pass

    def getDevId(self):
        return self.__device_id

    def getClaveID(self):
        return self.__claveID

    def getType(self):
        return 'AutoClaveRealTimeDataSet'

    def pushData(self, val):
        if val.getType() == 'AutoClaveRealTimeData':
            self.__recordList.append(val)
        else:
            print('[AutoClaveRealTimeDataSet] Data Type Error')

    def getSet(self, type):
        if type == 'json':
            recordListJson = []
            for data in self.__recordList:
                recDict = {'time': data.getTime(), 'inTemp': int(100 * data.getInTemp()),
                           'outTemp': int(100 * data.getOutTemp()), 'inPress': int(100 * data.getInPress()),
                           'state': data.getState()}
                recordListJson.append(recDict)
            obsRecDict = {'claveId': self.getClaveID(), 'lastTime': self.getLastTime(), 'records': recordListJson}
            return json.dumps(obsRecDict)

        elif type == 'numpy':
            return self.__toNumPy(self.__recordList, [1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6])

        elif type == 'list:':
            return self.__recordList

    def getLastTime(self):
        if len(self.__recordList) > 0:
            return self.__recordList[-1].getTime()
        else:
            return 0

    def __toNumPy(self, recordList, window):
        # 得到实时数据
        timeList = []
        inTempList = []
        outTempList = []
        inPressList = []
        stateList = []

        for autoClaveData in recordList:
            time = int(autoClaveData.getTime())

            inTemp = autoClaveData.getInTemp()
            outTemp = autoClaveData.getOutTemp()
            inPress = autoClaveData.getInPress()
            state = autoClaveData.getState()

            timeList.append(time)
            inTempList.append(inTemp)
            outTempList.append(outTemp)
            inPressList.append(inPress)
            stateList.append(state)

            # print("Id:"+str(claveId)+" T:"+str(time)+" iT:"+str(inTemp)+" oT:"+str(outTemp)+" iP:"+str(inPress)+" S:"+str(state))

        dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList])
        start = int(len(window) / 2)
        conv1 = np.convolve(dataSet[1, :], window)
        conv2 = np.convolve(dataSet[2, :], window)
        conv3 = np.convolve(dataSet[3, :], window)
        dataSet[1, :] = conv1[start:start + dataSet.shape[1]]
        dataSet[2, :] = conv2[start:start + dataSet.shape[1]]
        dataSet[3, :] = conv3[start:start + dataSet.shape[1]]
        return dataSet


# 蒸压釜实时数据
class AutoClaveRealTimeDataSet(DataSet):
    __AutoClaveDataSetList = []
    __AutoClaveNum = 0

    def __init__(self, claveNum):
        self.__AutoClaveNum = claveNum
        self.__AutoClaveDataSetList = []
        for claveId in range(1, self.__AutoClaveNum + 1):
            self.__AutoClaveDataSetList.append(SingleAutoClaveRealtimeDataSet(claveId, dev_prefix + str(claveId)))

    def getType(self):
        return 'AutoClaveRealTimeDataSet'

    def pushData(self, ID, val):
        if val.getType() == 'AutoClaveRealTimeData':
            self.__AutoClaveDataSetList[ID - 1].pushData(val)
        else:
            print('[AutoClaveRealTimeDataSet] Data Type Error')

    def getDevId(self, ID):
        return self.__AutoClaveDataSetList[ID - 1].getDevId()

    def getClaveNum(self):
        return self.__AutoClaveNum

    def getSet(self, ID):
        return self.__AutoClaveDataSetList[ID - 1]

    def getLastTime(self, ID):
        if len(self.__AutoClaveDataSetList[ID - 1].getSet()) > 0:
            return self.__AutoClaveDataSetList[ID - 1].getSet()[-1].getTimeStemp()
        else:
            return 0


# -*- coding:utf-8 -*-
import json
from src.src.Data import AutoClaveRealTimeData
from src.model.Algorithm import Algorithm
from configparser import ConfigParser
from datetime import datetime


class ACGetDataDIS(Algorithm):

    def __init__(self, DISTool, dataObj):

        self.DISTool = DISTool
        self.dataObj = dataObj

    def getType(self):
        return "ACGetDataDIS"

    def run(self):

        sourceData = self.DISTool.getRecords_test()
        conf = ConfigParser()
        conf.read(self.DISTool.getConfPath())
        # print(sourceData)

        for claveId in range(1, self.dataObj.getClaveNum() + 1):
            # Use configuration file

            inTempChannel = conf.get('AutoClave' + str(claveId), 'inTempChannel')
            inTempSlope = conf.get('AutoClave' + str(claveId), 'inTempSlope')
            inTempShift = conf.get('AutoClave' + str(claveId), 'inTempShift')

            outTempChannel = conf.get('AutoClave' + str(claveId), 'outTempChannel')
            outTempSlope = conf.get('AutoClave' + str(claveId), 'outTempSlope')
            outTempShift = conf.get('AutoClave' + str(claveId), 'outTempShift')

            inPressChannel = conf.get('AutoClave' + str(claveId), 'inPressChannel')
            inPressSlope = conf.get('AutoClave' + str(claveId), 'inPressSlope')
            inPressShift = conf.get('AutoClave' + str(claveId), 'inPressShift')

            stateChannel = conf.get('AutoClave' + str(claveId), 'stateChannel')

            oldTime = 0
            oldInTemp = 0.0
            oldOutTemp = 0.0
            oldInPress = 0.0

            for devRec in sourceData:
                data = json.loads(devRec['data'])
                dev_id = data['device_id']
                if dev_id == self.dataObj.getDevId(claveId):
                    services = data['services'][0]
                    properties = services['properties']
                    try:
                        time = datetime.strptime(services['event_time'], '%Y%m%dT%H%M%SZ').timestamp()
                        time = time + 3600 * 8

                        recData = AutoClaveRealTimeData(claveId)
                        recData.setTime(time)
                        inTemp = float(properties[inTempChannel]) * float(inTempSlope) + float(inTempShift)
                        outTemp = float(properties[outTempChannel]) * float(outTempSlope) + float(outTempShift)
                        inPress = float(properties[inPressChannel]) * float(inPressSlope) + float(inPressShift)

                        recData.setInTemp(inTemp)
                        recData.setOutTemp(outTemp)
                        recData.setInPress(inPress)

                        recData.setState(float(properties[stateChannel]))

                        oldInTemp = inTemp
                        oldOutTemp = outTemp
                        oldInPress = inPress
                        oldTime = time

                        self.dataObj.pushData(claveId, recData)
                    except:
                        pass

        return self.dataObj



class ACRealTimeOBS(Algorithm):

    def __init__(self, OBSTool, dataObj):
        self.OBSTool = OBSTool
        self.dataObj = dataObj

    def getType(self):
        return "ACRealTime"

    def run(self):
        for claveId in range(1, self.dataObj.getClaveNum() + 1):
            recordJson = self.dataObj.getSet(claveId).getSet('json')
            obsRecPrefix = 'Service/ZyRealTime/clave' + str(claveId)
            self.OBSTool.deleteObject(obsRecPrefix)
            self.OBSTool.writeContent(obsRecPrefix, str(recordJson))

import sys

# DIS数据工具
class DISDataTool(DataTool):
    print(sys.path)

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
            r = self.cli.getCursor(streamName=self.__streamName, partitionId=self.__partitionId,
                                   cursorType='TRIM_HORIZON', startSeq=self.__startSeq)
            return r.cursor
        except Exception as ex:
            print('DISDataMan __getCursor_test ' + str(ex))

    # Download data
    def getRecords_test(self):
        cursor = self.getCursor_test()
        print('cursor ' + cursor)
        records = []
        try:
            while cursor:
                r = self.cli.getRecords(partitioncursor=cursor)
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
            print('DISDataMan __getRecords_test: ' + str(ex))

    def getType(self):
        return 'DISDataTool'

    def getData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            return ACGetDataDIS(self, dataObj).run()

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


from datetime import timedelta
from datetime import datetime
import json

folderPath = 'Service/ZyRecord/'


class ACRecordInitOBS:

    def __init__(self, OBSTool, dataObj):
        self.OBSTool = OBSTool
        self.dataObj = dataObj

    def getType(self):
        return "ACRecordOBS"

    def run(self):
        nowTime = self.dataObj.getNowTime()  # 真实时间
        claveNum = self.dataObj.getClaveNum()
        offSetTime = nowTime - nowTime.tzinfo.utcoffset(nowTime)  # 减去7个小时的时间（今天上午七点前是昨天）
        offSetHour = int(nowTime.tzinfo.utcoffset(nowTime).total_seconds() / 3600)
        todayInitial = datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day, hour=offSetHour, minute=0,
                                second=0)
        todayFolderPath = folderPath + offSetTime.date().isoformat() + '/'
        todayRecordList = self.OBSTool.listObject(todayFolderPath)
        if (len(todayRecordList) > 0):  # 有今天的
            for claveId in range(1, claveNum + 1):
                for content in todayRecordList:
                    Xindex = content.key.find("X")
                    if (content.key[Xindex - 1:Xindex] == str(claveId)):
                        self.dataObj.getSet(claveId).pushData(content.key)
                        if content.key[Xindex + 1:Xindex + 4] == 'ING':
                            self.OBSTool.deleteObject(content.key)

            print('hasToday')
        else:  # 没有今天的
            print('noToday')

            offSetTimeYestd = offSetTime - timedelta(days=1)
            yestdFolderPath = folderPath + offSetTimeYestd.date().isoformat() + '/'
            yestdRecordList = self.OBSTool.listObject(yestdFolderPath)
            if (len(yestdRecordList) > 0):  # 有昨天的
                for claveId in range(1, claveNum + 1):
                    flag = 0
                    for content in yestdRecordList:
                        Xindex = content.key.find("X")
                        if (content.key[Xindex - 1:Xindex] == str(claveId)):
                            if content.key[Xindex + 1:Xindex + 4] == 'ING':
                                newTodayKey = todayFolderPath + content.key[Xindex - 1:]
                                self.OBSTool.copyObject(content.key, newTodayKey)
                                self.dataObj.getSet(claveId).pushData(newTodayKey)

                                flag = 1
                    if (flag == 0):
                        self.newRecord(claveId, todayInitial, offSetTime)

            else:  # 没有昨天的
                for claveId in range(1, claveNum + 1):
                    self.newRecord(claveId, todayInitial, offSetTime)

        return self.dataObj

        # 查找是否有今天的文件夹，如果没有就新建，并且去昨天的找，如果昨天也没有，那么就从7点算起建立新事件

    def backSearch(self, data):

        timeStemp = 0

        return timeStemp

    def newRecord(self, claveId, todayInitial, offSetTime):

        todayFolderPath = folderPath + offSetTime.date().isoformat() + '/'
        eventPrefix = todayFolderPath + str(claveId) + 'XING' + str(int(todayInitial.timestamp()) + claveId) + 'Y'
        print('newRecord: ' + eventPrefix)
        self.dataObj.getSet(claveId).pushData(eventPrefix)

        # recordJson = json.dumps(recordDict)
        # self.OBSTool.writeContent(prefix = eventPrefix, metaData = str(recordJson)) #新建今日文件夹


from datetime import timedelta
from datetime import datetime
import json

folderPath = 'Service/ZyRecord/'


class ACRecordOBS:

    def __init__(self, OBSTool, dataObj):
        self.OBSTool = OBSTool
        self.dataObj = dataObj

    def getType(self):
        return "ACRecordOBS"

    def run(self):
        nowTime = self.dataObj.getNowTime()  # 真实时间
        claveNum = self.dataObj.getClaveNum()
        offSetTime = nowTime - nowTime.tzinfo.utcoffset(nowTime)  # 减去7个小时的时间（今天上午七点前是昨天）
        todayFolderPath = folderPath + offSetTime.date().isoformat() + '/'

        for claveId in range(1, claveNum + 1):
            singleClaveEventList = self.dataObj.getSet(claveId).getSet()
            for event in singleClaveEventList:
                print('writeEvent ' + str(event.getPrefix()))
                if (event.getSet('json') != 0):
                    print('okwrtie ' + str(event.getPrefix()))
                    self.OBSTool.writeContent(prefix=todayFolderPath + event.getPrefix(),
                                              metaData=str(event.getSet('json')))  # 新建今日文件夹


# !/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
import sys

from lib.obs import ObsClient, Object, DeleteObjectsRequest, PutObjectHeader
from configparser import ConfigParser

# OBS数据人
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
        # 返回一个包含当日事件列表的对象，并包含数据指针，如果没有当日，会做当日初始化
        if dataObj.getType() == 'AutoClaveRecordDataSet':
            dataObj = ACRecordInitOBS(self, dataObj).run()
            return dataObj

        elif dataObj.getType() == 'AutoClaveRealTimeDataSet':

            pass

    def postData(self, dataObj):
        if dataObj.getType() == 'AutoClaveRealTimeDataSet':
            ACRealTimeOBS(self, dataObj).run()
        elif dataObj.getType() == 'AutoClaveRecordDataSet':
            ACRecordOBS(self, dataObj).run()

    def setConfPath(self, val):
        self.__confPath = val

    def setBucketName(self, val):
        self.__bucketName = val

    def deleteObject(self, prefix):
        resp = self.__obsClient.deleteObject(self.__bucketName, prefix)
        if resp.status < 300:
            print('Delete object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, 'prefix ', prefix, ',errorCode:', resp.errorCode, ',errorMessage:',
                  resp.errorMessage)

    def copyObject(self, fromPrefix, toPrefix):
        resp = self.__obsClient.copyObject(self.__bucketName, fromPrefix, self.__bucketName, toPrefix)

        if resp.status < 300:
            print('Copy Success!')
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)

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

        resp = self.__obsClient.putContent(self.__bucketName, prefix, metaData)
        if resp.status < 300:
            print('Create object ' + prefix + ' successfully!\n')
        else:
            print('common msg:status:', resp.status, ',errorCode:', resp.errorCode, ',errorMessage:', resp.errorMessage)

    def listObjectsInFolder(self, folderPrefix):

        resp = self.__obsClient.listObjects(self.__bucketName, folderPrefix)
        for content in resp.body.contents:
            print('\t' + content.key + ' etag[' + content.etag + ']')


import sys


sys.path.append('..\\lib')
from lib.obs import ObsClient, Object, DeleteObjectsRequest
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
            ak = conf.get('OBSconfig', 'ak')
            sk = conf.get('OBSconfig', 'sk')
            server = conf.get('OBSconfig', 'server')
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


import abc
import sys
sys.path.append('..\\lib')
from lib.DIS.client import disclient
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
            projectid = conf.get('DISconfig', 'projectid')
            ak = conf.get('DISconfig', 'ak')
            sk = conf.get('DISconfig', 'sk')
            region = conf.get('DISconfig', 'region')
            endpoint = conf.get('DISconfig', 'endpoint')

            try:
                dis = disclient.disclient(endpoint=endpoint, ak=ak, sk=sk, projectid=projectid, region=region)
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


print('okok')


# 蒸压釜记录
class AutoClaveRecordData(Data):
    __claveID = 0
    __inTemp = 0
    __inTempDiff = 0

    __outTemp = 0
    __outTempDiff = 0

    __inPress = 0
    __inPressDiff = 0

    __time = 0
    __state = 0
    __stateName = ''

    def __init__(self, ID, time):
        self.__claveID = ID
        self.__time = time

    def getType(self):
        return 'AutoClaveRecordData'

    def getTime(self):
        return self.__time

    def setTime(self, time):
        self.__time = time

    def getInTemp(self):
        return self.__inTemp

    def getInTempDiff(self):
        return self.__inTempDiff

    def setInTemp(self, inTemp, inTempDiff):
        self.__inTemp = round(inTemp, 3)
        self.__inTempDiff = round(inTempDiff, 3)

    def getOutTemp(self):
        return self.__outTemp

    def getOutTempDiff(self):
        return self.__outTempDiff

    def setOutTemp(self, outTemp, outTempDiff):
        self.__outTemp = round(outTemp, 3)
        self.__outTempDiff = round(outTempDiff, 3)

    def getInPress(self):
        return self.__inPress

    def getInPressDiff(self):
        return self.__inPressDiff

    def setInPress(self, inPress, inPressDiff):
        self.__inPress = round(inPress, 3)
        self.__inPressDiff = round(inPressDiff, 3)

    def getState(self, para):
        if para == 'name':
            return self.__stateName
        else:
            return self.__state

    def setState(self, state, stateName):
        self.__state = state
        self.__stateName = stateName


# -*- coding:utf-8 -*-
import json
import numpy as np
from configparser import ConfigParser
from datetime import datetime
from datetime import timezone
from datetime import timedelta


class ACTimeDomainAnalysisOBS(Algorithm):

    def __init__(self, OBSTool, dataObj, realTimeRecord):

        self.OBSTool = OBSTool
        self.dataObj = dataObj
        self.realTimeRecord = realTimeRecord

    def getType(self):
        return "ACTimeDomainAnalysisOBS"

    def run(self):
        ifEnd = []
        for claveId in range(1, self.realTimeRecord.getClaveNum() + 1):
            print('for clave:' + str(claveId))
            startTime = 0
            oldState = ""

            # 找到最新纪录时间，分两种情况，1是全部是FIN，所以开始时间算作最后一个FIN的结束时间
            # 2是有ING，有且仅有一个ING，开始时间算作ING的开始时间
            for event in self.dataObj.getSet(claveId).getSet():
                prefix = event.getPrefix()
                Xindex = prefix.find("X")
                Yindex = prefix.find("Y")
                if (prefix[Xindex + 1:Xindex + 4] == "ING"):
                    time = int(prefix[Xindex + 4:Yindex])
                    startTime = time
                    oldState = "ING"
                    break
                elif (prefix[Xindex + 1:Xindex + 4] == "FIN"):
                    time = int(prefix[Yindex + 1:])
                    if time > startTime:
                        startTime = time
                        oldState = "FIN"

            # 实时record list
            dataSet = self.realTimeRecord.getSet(claveId).getSet('numpy')

            print('ClaveId ' + str(claveId) + ' sttime: ' + str(startTime) + '  oldState: ' + oldState)

            # 如果上一次记录的state是FIN， 要找新的事件, 没有新的事件则不录
            time = 0
            if oldState == 'FIN':
                time = self.__startEventDetect(dataSet, startTime, 0.05)
                if (time > 0):
                    ev = self.dataObj.newEvent('XING', claveId)
                    ev = self.__writeContent(claveId, dataSet, ev, time, 0)
                    print('evPrefix newIng: ' + ev.getPrefix() + ' time ' + str(time))
                    self.dataObj.getSet(claveId).pushData(ev)

            # 如果上一次记录的state是ING，则判断是否结束， 若没有结束就更新数据， 有结束则新建FIN
            elif oldState == 'ING':
                time = self.__endEventDetect(dataSet, startTime, 0.05)
                ev = self.__writeContent(claveId, dataSet, event, startTime, time)
                print('evPrefix refreshING/New FIN: ' + ev.getPrefix() + ' time ' + str(time))
                self.dataObj.getSet(claveId).pushData(ev)
                if time == 0:
                    ifEnd.append(1)

        total = 0
        ifAllEnd = 0
        for ele in range(0, len(ifEnd)):
            total = total + ifEnd[ele]

        if total == self.realTimeRecord.getClaveNum():
            ifAllEnd = 1

        return self.dataObj, ifAllEnd

    def __writeContent(self, claveId, dataSet, event, startTime, endTime):
        if event.getType() == 'SingleAutoClaveRecordEvent':
            event.setStartTime(startTime)
            event.setEndTime(endTime)

            if (endTime == 0):
                endTime = 1000000000000000
                event.setPrefix(str(claveId) + "XING" + str(startTime) + "Y")
            else:
                event.setPrefix(str(claveId) + "XFIN" + str(startTime) + "Y" + str(endTime))

            for data in np.nditer(dataSet, flags=['external_loop'], order='F'):

                if data[0] >= startTime and data[0] <= endTime:
                    dataObj = AutoClaveRecordData(event.getClaveId(), data[0])

                    dataObj.setInTemp(data[1])
                    dataObj.setOutTemp(data[2])
                    dataObj.setInPress(data[3])
                    dataObj.setState(self.__getState(data[4]))

                    event.pushData(dataObj)

            return event
        else:
            print('DataType Error')

    # 要求  1，如果state有记录，startTime选在 state变为关门的那一刻的前5分钟
    #      2，如果state没有记录， startTime选在之前蒸养结束后5分钟
    def __startEventDetect(self, dataSet, startTime, tresh):

        time_a = 0
        ts = 5
        j = ts

        while dataSet[:, j][0] <= startTime and j < dataSet.shape[1] - 1:
            j = j + 1

        if dataSet[:, j][3] >= tresh:
            while dataSet[:, j][3] >= tresh and j > ts:
                j = j - 1
        else:
            pass
        while not (dataSet[:, j][3] > tresh or self.__getState(dataSet[:, j + 1][4]) != self.__getState(
                dataSet[:, j][4])) and j > ts:
            j = j - 1
        time_a = dataSet[:, j - ts][0]

        return int(time_a)

    # 要求  1，如果state有记录，startTime选在 state变为开门的那一刻的后5分钟
    #      2，如果state没有记录， startTime选在之前蒸养开始前5分钟
    def __endEventDetect(self, dataSet, startTime, tresh):

        time_a = 0
        ts = 5
        j = ts

        while dataSet[:, j][0] <= startTime and j < dataSet.shape[1] - 1:
            j = j + 1

        while j < dataSet.shape[1] - 1 and (dataSet[:, j][3] - dataSet[:, j - 1][3] < 0 or dataSet[:, j][3] <= tresh):
            j = j + 1

        while j < dataSet.shape[1] - 1 and (dataSet[:, j][3] - dataSet[:, j - 1][3] >= 0 or dataSet[:, j][3] > tresh):
            j = j + 1

        while j < dataSet.shape[1] - 1 and (
                dataSet[:, j][3] - dataSet[:, j - 1][3] < 0 or dataSet[:, j][3] <= tresh) and (
                self.__getState(dataSet[:, j + 1][4]) * self.__getState(dataSet[:, j][4]) != 12):
            j = j + 1

        if (j >= dataSet.shape[1] - 2):
            time_a = 0
        else:
            if (dataSet[:, j][3] - dataSet[:, j - 1][3] > 0 and dataSet[:, j][3] >= tresh):
                time_a = dataSet[:, j - ts][0]

            if (self.__getState(dataSet[:, j + 1][4]) * self.__getState(dataSet[:, j][4]) == 12):
                time_a = dataSet[:, j + ts][0]

        return int(time_a)

    def __getState(self, val):
        offset = 186
        val = val + offset
        itv = 372
        if val > 0 and val <= 1 * itv:
            return 1  # 0
        elif val > 1 * itv and val <= 2 * itv:
            return 2
        elif val > 2 * itv and val <= 3 * itv:
            return 3
        elif val > 3 * itv and val <= 4 * itv:
            return 4
        elif val > 4 * itv and val <= 5 * itv:
            return 5
        elif val > 5 * itv and val <= 6 * itv:
            return 6
        elif val > 6 * itv and val <= 7 * itv:
            return 7
        elif val > 7 * itv and val <= 8 * itv:
            return 8
        elif val > 8 * itv and val <= 9 * itv:
            return 9
        elif val > 9 * itv and val <= 10 * itv:
            return 10
        elif val > 10 * itv and val <= 11 * itv:
            return 11
        elif val > 11 * itv and val <= 4095 + offset:
            return 12


import sys
import abc
import time
import json
import os
import sys
import schedule
from datetime import datetime
from datetime import timezone
from datetime import timedelta


# 任务抽象类
class DIStoOBSscheduleTask(ScheduleTask):
    __period = 0

    def __init__(self, period):
        self.__period = period

    def __job(self):

        # 从DIS采集蒸压釜实时数据，并上传到OBS
        claveNum = 7
        disDataTool = DISDataToolFactory().newObject('shardId-0000000000', 0, 'dis-YDY1')
        # dataSet = 0
        dataSet = AutoClaveRealTimeDataSet(claveNum)
        dataSet = disDataTool.getData(dataSet)

        obsDataTool = OBSDataToolFactory().newObject('obs-ydy1')
        obsDataTool.postData(dataSet)

        hourOffset = 7
        today = datetime.today()
        nowTime = datetime(year=today.year, month=today.month, day=today.day, hour=8, minute=0, second=0, microsecond=0,
                           tzinfo=timezone(timedelta(hours=hourOffset)), fold=0)
        autoClaveRecordTask = AutoClaveEventRecordTask(nowTime, dataSet, claveNum, obsDataTool)
        autoClaveRecordTask.run()

    def setPeriod(self, period):
        self.__period = period

    def getType(self):
        return "DIStoOBSscheduleTask"

    def run(self, para):

        if para == 1:
            self.__job()
        else:
            schedule.every(self.__period).minutes.do(self.__job)
            while True:
                schedule.run_pending()
                time.sleep(1)


# 任务抽象类
class AutoClaveEventRecordTask(Task):

    def __init__(self, nowTime, dataSet, claveNum, obsDataTool):
        self.nowTime = nowTime
        self.dataSet = dataSet
        self.claveNum = claveNum
        self.obsDataTool = obsDataTool

    def getType(self):
        return "AutoClaveEventRecordTask"

    def run(self):
        maxIter = 10
        iter = 0
        ifAllEnd = 0
        while ifAllEnd == 0 and iter < maxIter:
            oldAutoClaveRecord = AutoClaveRecordDataSet(self.claveNum, self.nowTime)
            oldAutoClaveRecord = self.obsDataTool.getData(oldAutoClaveRecord)
            newAutoClaveRecord, ifAllEnd = ACTimeDomainAnalysisOBS(self, oldAutoClaveRecord, self.dataSet).run()
            self.obsDataTool.postData(newAutoClaveRecord)
            time.sleep(1)


#5分钟一次
disToObsSchedule = DIStoOBSscheduleTask(5)
disToObsSchedule.run(1)



