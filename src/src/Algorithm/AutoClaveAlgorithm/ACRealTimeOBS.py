# -*- coding:utf-8 -*-
import json
from model.Algorithm import Algorithm

class ACRealTimeOBS(Algorithm):

    def __init__(self, OBSTool, dataObj):

        self.OBSTool = OBSTool
        self.dataObj = dataObj

    
    def getType(self):
        return "ACRealTime"

    def run(self):
        
        for claveId in range(1,self.dataObj.getClaveNum()+1):
            record = self.dataObj.getSet(claveId)
            recordList = []
            for data in record.getSet():
                recDict = {'time':data.getTime(), 'inTemp':data.getInTemp(), 'outTemp':data.getOutTemp(), 'inPress':data.getInPress(), 'state':data.getState()}
                recordList.append(recDict)
            obsRecDict = {'claveId':claveId, 'lastTime':record.getLastTime(), 'records':recordList}
            obsRecPrefix = 'Service/ZyRealTime/clave'+str(claveId)
            print('OBS write len:'+str(len(recordList))+' devId:'+self.dataObj.getDevId(claveId))
            self.OBSTool.deleteObject(obsRecPrefix)
            self.OBSTool.writeContent(obsRecPrefix, str(json.dumps(obsRecDict)))