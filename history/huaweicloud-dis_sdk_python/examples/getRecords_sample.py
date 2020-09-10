#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import os
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

streamname = "dis-w_p"
startSeq='0'
partitionId="shardId-0000000001"



def test():
    try:
        return disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
    except Exception as ex:
        print(str(ex))



# enter getCursor endpoint information
def getCursor_test():
    try:
        cli=test()
        r = cli.getCursor(streamName=streamname,partitionId=partitionId,cursorType='AFTER_SEQUENCE_NUMBER',startSeq=startSeq)
        return r.cursor
    except Exception as ex:
        print(str(ex))


# Download data
def getRecords_test():
    cli=test()
    cursor=getCursor_test()
    try:
        while cursor:
            r = cli.getRecords(partitioncursor=cursor)
            cursor = r.nextPartitionCursor
            if r.recordResult == []:
                break

            print(r.statusCode)
            # print(r.recordResult)

            if IS_PYTHON2:
                print(json.dumps(r.body))
            else:
                print(r.body)

            # for i in r.getRecordResult(r.recordResult):
            #     print("%s %s" % (i.sequence_number, i.data))


    except Exception as ex:
        print(str(ex))



if __name__ == '__main__':
    print("Use your Stream to get data")
    getRecords_test()


