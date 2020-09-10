#-*-coding:utf-8-*-
#!/usr/bin/python

import json
import sys
sys.path.append('../')
from src.com.dis.client import disclient
from src.com.dis.models.base_model import IS_PYTHON2
if IS_PYTHON2:
    from ConfigParser import ConfigParser
else:
    from configparser import ConfigParser

fp='../conf.ini'
conf=ConfigParser()
conf.read(fp)
# Use configuration file
try:
    projectid = conf.get('Section1','projectid')
    ak = conf.get('Section1','ak')
    sk = conf.get('Section1','sk')
    region = conf.get('Section1','region')
    endpoint = conf.get('Section1','endpoint')
except Exception as ex:
    print(str(ex))


# projectid = "your projectid"
# endpoint = " "
# ak = "*** Provide your Access Key ***"
# sk = "*** Provide your Secret Key ***"
# region = " "

streamname = " "
startSeq='0'
partitionId=" "
cursorType='AT_SEQUENCE_NUMBER'

def test():
    try:
        return disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
    except Exception as ex:
        print(str(ex))


# enter getCursor endpoint information
def getCursor_test():
    try:
        cli=test()
        r = cli.getCursor(streamName=streamname,partitionId=partitionId,cursorType=cursorType,startSeq=startSeq)
        print(r.statusCode)
        if IS_PYTHON2:
            print(json.dumps(r.body))
        else:
            print(r.body)
    except Exception as ex:
        print(str(ex))



if __name__ == '__main__':
    print("Use your Stream to get Cursor")
    getCursor_test()

