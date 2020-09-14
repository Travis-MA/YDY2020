import abc

class Algorithm(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def getType(self):
        pass

    @abc.abstractmethod 
    def run(self, para):
        pass
