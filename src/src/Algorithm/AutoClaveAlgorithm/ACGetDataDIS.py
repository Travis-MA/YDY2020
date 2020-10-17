# -*- coding:utf-8 -*-
import json
from src.Data import AutoClaveRealTimeData
from model.Algorithm import Algorithm
from configparser import ConfigParser
from datetime import datetime

class ACGetDataDIS(Algorithm):

    def __init__(self, DISTool, dataObj):

        self.DISTool = DISTool
        self.dataObj = dataObj

    
    def getType(self):
        return "ACGetDataDIS"

    def run(self):
        
        sourceData = self.DISTool.getRecords_test()
        conf = ConfigParser()
        conf.read(self.DISTool.getConfPath())
        #print(sourceData)

        for claveId in range(1, self.dataObj.getClaveNum()+1):
            # Use configuration file

            inTempChannel = conf.get('AutoClave'+str(claveId),'inTempChannel')
            inTempSlope = conf.get('AutoClave'+str(claveId),'inTempSlope')
            inTempShift = conf.get('AutoClave'+str(claveId),'inTempShift')
                            
            outTempChannel = conf.get('AutoClave'+str(claveId),'outTempChannel')
            outTempSlope = conf.get('AutoClave'+str(claveId),'outTempSlope')
            outTempShift = conf.get('AutoClave'+str(claveId),'outTempShift')

            inPressChannel = conf.get('AutoClave'+str(claveId),'inPressChannel')
            inPressSlope = conf.get('AutoClave'+str(claveId),'inPressSlope')
            inPressShift = conf.get('AutoClave'+str(claveId),'inPressShift')

            stateChannel = conf.get('AutoClave'+str(claveId),'stateChannel')  


            oldTime = 0
            oldInTemp = 0.0
            oldOutTemp = 0.0
            oldInPress = 0.0  

            for devRec in sourceData:
                data = json.loads(devRec['data'])
                dev_id = data['device_id']
                if dev_id == self.dataObj.getDevId(claveId): 
                    services = data['services'][0]
                    properties = services['properties']
                    try:
                        time = datetime.strptime(services['event_time'],'%Y%m%dT%H%M%SZ').timestamp()
                        time = time + 3600*8
                    
                        recData = AutoClaveRealTimeData(claveId)
                        recData.setTime(time)
                        inTemp = float(properties[inTempChannel]) * float(inTempSlope) + float(inTempShift)
                        outTemp = float(properties[outTempChannel]) * float(outTempSlope) + float(outTempShift)
                        inPress = float(properties[inPressChannel]) * float(inPressSlope) + float(inPressShift)

                        recData.setInTemp(inTemp)
                        recData.setOutTemp(outTemp)
                        recData.setInPress(inPress)

                        recData.setState(float(properties[stateChannel]))
                        self.dataObj.pushData(claveId, recData)
                    except:
                        pass
    

        return self.dataObj