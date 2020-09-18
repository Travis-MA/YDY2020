import abc
from model.Data import Data
print('okok')

#蒸压釜记录
class AutoClaveRecordData(Data):

    __claveID = 0
    __inTemp = 0
    __inTempDiff = 0

    __outTemp = 0
    __outTempDiff = 0

    __inPress = 0
    __inPressDiff = 0

    __time = ''
    __state = 0
    __stateName = ''

    def __init__(self, ID):
        self.__claveID = ID
   
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
        self.__inTemp = inTemp
        self.__inTempDiff = inTempDiff

    def getOutTemp(self):
        return self.__outTemp

    def getOutTempDiff(self):
        return self.__outTempDiff
    
    def setOutTemp(self, outTemp, outTempDiff):
        self.__outTemp = outTemp
        self.__outTempDiff = outTempDiff

    def getInPress(self):
        return self.__inPress

    def getInPressDiff(self):
        return self.__inPressDiff

    def setInPress(self, inPress ,inPressDiff):
        self.__inPress = inPress
        self.__inPressDiff = inPressDiff

    def getState(self, para):
        if para == 'name':
            return self.__stateName
        else:
            return self.__state


    def setState(self, state, stateName):
        self.__state = state
        self.__stateName = stateName



