import abc
import json
from model.DataSet import DataSet
print('okok')

dev_prefix = '5f2149f4f958e402cae59c57_00'

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
        if val.getType() == 'AutoClaveData':
            self.__recordList.append(val)
        else:
            print('[AutoClaveRealTimeDataSet] Data Type Error')

    def getSet(self, type):
        if type == 'json':
            recordListJson = []
            for data in self.__recordList:
                recDict = {'time':data.getTime(), 'inTemp':data.getInTemp(), 'outTemp':data.getOutTemp(), 'inPress':data.getInPress(), 'state':data.getState()}
                recordListJson.append(recDict)
            obsRecDict = {'claveId':self.getClaveID(), 'lastTime':self.getLastTime(), 'records':recordListJson}
            return json.dumps(obsRecDict)
        else:
            return self.__recordList

    def getLastTime(self):
        if len(self.__recordList) > 0:
            return self.__recordList[-1].getTime()
        else:
            return 0


#蒸压釜实时数据
class AutoClaveRealTimeDataSet(DataSet):

    __AutoClaveDataSetList = []
    __AutoClaveNum = 0

    def __init__(self, claveNum):
        self.__AutoClaveNum = claveNum
        self.__AutoClaveDataSetList = []
        for claveId in range(1,self.__AutoClaveNum+1):
            self.__AutoClaveDataSetList.append(SingleAutoClaveRealtimeDataSet(claveId, dev_prefix+str(claveId)))

    def getType(self):
        return 'AutoClaveRealTimeDataSet'

    def pushData(self, ID, val):
        if val.getType() == 'AutoClaveData':
            self.__AutoClaveDataSetList[ID-1].pushData(val)
        else:
            print('[AutoClaveRealTimeDataSet] Data Type Error')
    
    def getDevId(self, ID):
        return self.__AutoClaveDataSetList[ID-1].getDevId()

    def getClaveNum(self):
        return self.__AutoClaveNum

    def getSet(self, ID):
        return self.__AutoClaveDataSetList[ID-1]


    def getLastTime(self, ID):
        if len(self.__AutoClaveDataSetList[ID-1].getSet()) > 0:
            return self.__AutoClaveDataSetList[ID-1].getSet()[-1].getTimeStemp()
        else:
            return 0
