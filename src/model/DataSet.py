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

#实时数据RealTime抽象类，子类类型包括所有设备的实时数据，如蒸压釜实时数据AutoClaveRealTimeData 等。
class RealTimeDataSet(DataSet, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getLastTime(self):
        pass

#记录Record抽象类，子类类型包括所有设备的数据记录，需要指定起止时间
class RecordDataSet(DataSet, metaclass = abc.ABCMeta):

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
class ListDataSet(DataSet, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setFatherList(self,fatherList):
        pass

    @abc.abstractmethod
    def getFatherList(self):
        pass
