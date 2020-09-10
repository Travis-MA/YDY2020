import abc
from model.DataSet import RealTimeDataSet
print('okok')


class SingleAutoClaveDataSet(RealTimeDataSet):

    __recordList = []
    __claveID = 0
    __device_id = ''

    def __init__(self, ID, dev_ID):
        self.__claveID = ID
        self.__device_id = dev_ID
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

    def getSet(self):
        return self.__recordList

    def getLastTime(self):
        if len(self.__recordList) > 0:
            return self.__recordList[-1].getTimeStemp()
        else:
            return 0


#蒸压釜实时数据
class AutoClaveRealTimeDataSet(RealTimeDataSet):

    __AutoClaveDataSetList = []
    __AutoClaveNum = 0

    def __init__(self, claveNum):
        self.__AutoClaveNum = claveNum
        dev_prefix = '5f2149f4f958e402cae59c57_00'
        for claveId in range(1,self.__AutoClaveNum+1):
            self.__AutoClaveDataSetList.append(SingleAutoClaveDataSet(claveId, dev_prefix+str(claveId)))

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
