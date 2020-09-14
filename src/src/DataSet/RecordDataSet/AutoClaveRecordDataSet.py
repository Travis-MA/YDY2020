import abc
from model.DataSet import DataSet

class SingleAutoClaveRecordDataSet(DataSet):


    def __init__(self, claveId):
        self.eventList = []
        self.claveId = claveId


    def getType(self):
        return "SingleAutoClaveRecordDataSet"


    def pushData(self,data):
        pass


    def getSet(self):
        pass


#蒸压釜数据记录
class AutoClaveRecordDataSet(DataSet):

    def __init__(self, claveNum, nowTime):
        self.subRecordSetList = []
        self.nowTime = nowTime
        self.claveNum = claveNum
        for claveId in range (1 , self.claveNum+1):
            subRecordSet = SingleAutoClaveRecordDataSet(claveId)
            self.subRecordSetList.append(subRecordSet)

    def getRecordDate(self):
        return self.recordDate

    def getType(self):
        return "AutoClaveRecordDataSet"

    def getNowTime(self):
        return self.nowTime


    def pushData(self,data):
        pass


    def getSet(self):
        pass

