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


# stream_type =""/"FILE"
stream_type=""


def test():
    try:
        return disclient.disclient(endpoint,ak,sk,projectid,region)
    except Exception as ex:
        print(str(ex))


def Dump_switch():
    jsonBody = {
        "stream_name": "dis-w_p",
        "partition_count": 1,
        "stream_type": 'ADVANCED',
        'data_duration': 24,
        # 'data_type':"BLOB"/"JSON"/"CSV"
        'data_type': "JSON"
    }
    return jsonBody



def Dump_switch_FILE():
    jsonBody = {"stream_name": "file_test",
                "partition_count": 1,
                "stream_type": "COMMON",
                'data_duration': 24,
                'data_type': "FILE",
                'obs_destination_descriptor':[
                        {
                            'agency_name': "all",
                            'obs_bucket_path': "002"
                        }]
                }
    return jsonBody


#create your streamname
def createStream_test():
    cli=test()
    if stream_type == "FILE":
        jsonBody = Dump_switch_FILE()
    else:
        jsonBody = Dump_switch()

    streamname = jsonBody['stream_name']
    partitionCount = jsonBody['partition_count']
    try:
        r=cli.createStream(streamname,partitionCount,streamType="COMMON",jsonBody=jsonBody)
        print(r.statusCode)
        if r.statusCode==201:
            print('"%s" Created successfully !'%(streamname))
    except Exception as ex:
        print(str(ex))




if __name__ == '__main__':
    print("start createStream ")
    createStream_test()
