import abc
from model.Data import Data

#蒸压釜记录
class AutoClaveRecordData(Data):

    __claveID = 0
    __inTemp = 0
    __outTemp = 0
    __inPress = 0


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

    def setInTemp(self, inTemp):
        self.__inTemp = round(inTemp,3)

    def getOutTemp(self):
        return self.__outTemp

    def setOutTemp(self, outTemp):
        self.__outTemp = round(outTemp,3)


    def getInPress(self):
        return self.__inPress

    def setInPress(self, inPress):
        self.__inPress = round(inPress,3)

    def getState(self):

        return self.__state


    def setState(self, state):
        self.__state = state



