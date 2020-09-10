import abc
from model.DataSet import RecordDataSet

#蒸压釜数据记录
class AutoClaveRecordDataSet(RecordDataSet):

    def getType(self):
        return 'AutoClaveRecordData'

    def setStartTime(self, startTime):
        pass
    
    def addData(self, val):
        pass

    def setEndTime(self, startTime):
        pass


    def getStartTime(self):
        pass

    def getEndTime(self):
        pass

