import abc

#数据抽象类 
class Data(metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def getType(self):
        pass

