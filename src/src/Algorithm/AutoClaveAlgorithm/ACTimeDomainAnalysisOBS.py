# -*- coding:utf-8 -*-
import json
import src.Data.Data as Data
from model.Algorithm import Algorithm
from configparser import ConfigParser

class ACTimeDomainAnalysisOBS(Algorithm):

    def __init__(self, OBSTool, dataObj):

        self.OBSTool = OBSTool
        self.dataObj = dataObj

    
    def getType(self):
        return "ACTimeDomainAnalysisOBS"

    def run(self):
        
        sourceData = self.DISTool.getRecords_test()
        conf = ConfigParser()
        conf.read(self.DISTool.getConfPath())
        #print(sourceData)

        for claveId in range(1, self.dataObj.getClaveNum()+1):
            # Use configuration file
            try:
                inTempChannel = conf.get('AutoClave'+str(claveId),'inTempChannel')
                outTempChannel = conf.get('AutoClave'+str(claveId),'outTempChannel')
                inPressChannel = conf.get('AutoClave'+str(claveId),'inPressChannel')
                stateChannel = conf.get('AutoClave'+str(claveId),'stateChannel')    
                
                for devRec in sourceData:
                    data = json.loads(devRec['data'])
                    dev_id = data['device_id']
                    if dev_id == self.dataObj.getDevId(claveId): 
                        services = data['services'][0]
                        properties = services['properties']
                    
                        recData = Data.AutoClaveData(claveId)
                        recData.setTime(services['event_time'])
                        recData.setInTemp(float(properties[inTempChannel]))
                        recData.setOutTemp(float(properties[outTempChannel]))
                        recData.setInPress(float(properties[inPressChannel]))
                        recData.setState(float(properties[stateChannel]))


                        self.dataObj.pushData(claveId, recData)
                print('recDataLen: '+str(len(self.dataObj.getSet(claveId).getSet()))+' devid:'+self.dataObj.getDevId(claveId))
                    
            except Exception as ex:
                print('[DISDataTool](getData)' + str(ex))
                    
        return self.dataObj