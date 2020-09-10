import abc

#数据抽象类 包括三种子类类型（实时数据RealTime， 记录Record， 目录List）
class Data(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def setData(self):
        pass

    @abc.abstractmethod
    def getType(self):
        pass


#实时数据RealTime抽象类，子类类型包括所有设备的实时数据，如蒸压釜实时数据AutoClaveRealTimeData 等。
class RealTimeData(Data, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getLastTime(self):
        pass

#记录Record抽象类，子类类型包括所有设备的数据记录，需要指定起止时间
class RecordData(Data, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setStartTime(self, startTime):
        pass

    @abc.abstractmethod
    def setEndTime(self, startTime):
        pass


    @abc.abstractmethod
    def getStartTime(self):
        pass

    @abc.abstractmethod
    def getEndTime(self):
        pass

#目录List抽象类，子类类型包括所有设备数据储存的目录信息，必须指定上级目录
class ListData(Data, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setFatherList(self,fatherList):
        pass

    @abc.abstractmethod
    def getFatherList(self):
        pass

#蒸压釜实时数据
class AutoClaveRealTimeData(RealTimeData):

    __recordList = []
    
    def getType(self):
        return 'AutoClaveRealTimeData'

    def setData(self, val):
        self.__recordList = val

    def getRecordList(self):
        return self.__recordList

    def getLastTime(self):
        pass


#锅炉实时数据
class BoilerRealTimeData(RealTimeData):

    def getData(self):
        print('BoilerRealTimeData getData')

    def getDataJson(self):
        print('BoilerRealTimeData getDataJson')

    def getLastTime(self):
        print('BoilerRealTimeData getLastTime')

#蒸压釜数据记录
class AutoClaveRecordData(RecordData):

    def getType(self):
        return 'AutoClaveRecordData'

    def setStartTime(self, startTime):
        pass
    
    def setData(self, val):
        pass

    def setEndTime(self, startTime):
        pass


    def getStartTime(self):
        pass

    def getEndTime(self):
        pass

