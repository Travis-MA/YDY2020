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
            recordJson = self.dataObj.getSet(claveId).getSet('json')
            obsRecPrefix = 'Service/ZyRealTime/clave'+str(claveId)
            self.OBSTool.deleteObject(obsRecPrefix)
            self.OBSTool.writeContent(obsRecPrefix, str(recordJson))