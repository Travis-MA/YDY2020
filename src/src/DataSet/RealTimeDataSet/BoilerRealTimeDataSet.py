import abc
from model.DataSet import RealTimeDataSet

#锅炉实时数据
class BoilerRealTimeDataSet(RealTimeDataSet):

    def getData(self):
        print('BoilerRealTimeData getData')

    def getDataJson(self):
        print('BoilerRealTimeData getDataJson')

    def getLastTime(self):
        print('BoilerRealTimeData getLastTime')
