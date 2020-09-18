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
    
    def getInTempDiff(self):
        return self.__inTempDiff

    def setInTemp(self, inTemp, inTempDiff):
        self.__inTemp = round(inTemp,3)
        self.__inTempDiff = round(inTempDiff,3)

    def getOutTemp(self):
        return self.__outTemp

    def getOutTempDiff(self):
        return self.__outTempDiff
    
    def setOutTemp(self, outTemp, outTempDiff):
        self.__outTemp = round(outTemp,3)
        self.__outTempDiff = round(outTempDiff,3)

    def getInPress(self):
        return self.__inPress

    def getInPressDiff(self):
        return self.__inPressDiff

    def setInPress(self, inPress ,inPressDiff):
        self.__inPress = round(inPress,3)
        self.__inPressDiff = round(inPressDiff,3)

    def getState(self, para):
        if para == 'name':
            return self.__stateName
        else:
            return self.__state


    def setState(self, state, stateName):
        self.__state = state
        self.__stateName = stateName



