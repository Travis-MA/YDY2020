#!/usr/bin/python
# -*- coding:utf-8 -*-

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
metaData="  "
appName="abc"
partitionId="shardId-0000000000"
seqNumber="0"


# Initialize disclient
def test():
    try:
        return disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
    except Exception as ex:
        print(str(ex))


#commit your Checkpoint
def commitCheckpoint_test():
    cli = test()
    try:
        r=cli.commitCheckpoint(streamName=streamname,appName=appName,partitionId=partitionId,seqNumber=seqNumber,metaData=metaData)
        print(r.statusCode)
    except Exception as ex:
        print(str(ex))



if __name__ == '__main__':
    print("start commit your Checkpoint")
    commitCheckpoint_test()

