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

