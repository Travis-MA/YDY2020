import abc
from model.Data import Data
print('okok')

#蒸压釜实时数据
class AutoClaveData(Data):

    __claveID = 0
    __inTemp = 0
    __outTemp = 0
    __inPress = 0
    __time = ''
    __state = 0
    __stateName = ''

    def __init__(self, ID):
        self.__claveID = ID
   
    def getType(self):
        return 'AutoClaveData'

    def getTime(self):
        return self.__time
    
    def setTime(self, time):
        self.__time = time

    def getInTemp(self):
        return self.__inTemp

    def setInTemp(self, inTemp):
        self.__inTemp = inTemp

    def getOutTemp(self):
        return self.__outTemp
    
    def setOutTemp(self, outTemp):
        self.__outTemp = outTemp

    def getInPress(self):
        return self.__inPress

    def setInPress(self, inPress):
        self.__inPress = inPress

    def getState(self):
        return self.__state
    
    def setState(self, state):
        self.__state = state



