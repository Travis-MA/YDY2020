# !/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys,json
import time,random

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



'''
Enter the following information
'''
##projectid = "your projectid"
##endpoint = " "
##ak = "*** Provide your Access Key ***"
##sk = "*** Provide your Secret Key ***"
##region = " "
##streamname = "your stream_name"


streamname = "dis-c-test-partition-1"

def test():
    try:
        return disclient.disclient(endpoint=endpoint, ak=ak, sk=sk, projectid=projectid, region=region)
    except Exception as ex:
        print(str(ex))



def putRecords_test():
    cli = test()
    # [{"data": "xxxxx", "partition_key": '0'}]
    # [{"data": "xxxxx", "partition_id": '0'}]

    records = []
    record1 = {"data": "xxxxx", "partition_key": '0'}
    record2 = {"data": "xxxxx", "partition_key": '0'}
    records.append(record1)
    records.append(record2)

    try:

        r=cli.putRecords(streamname, records)

        print(r.statusCode)

        # print(r.recordResult)

        if IS_PYTHON2:
            print(json.dumps(r.body))
        else:
            print(r.body)


        # for i in r.getSendRecordResult(r.recordResult):
        #     print("%s %s %s %s" % (i.sequence_number, i.partition_id, i.error_code, i.error_message))

    except Exception as ex:
        print(str(ex))







if __name__ == '__main__':
    print("Use your Stream to putRecords")
    putRecords_test()










