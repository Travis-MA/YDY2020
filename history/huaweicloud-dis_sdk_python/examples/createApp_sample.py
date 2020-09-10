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


'''
Enter the following information

'''
# projectid = "your projectid"
# endpoint = " "
# ak = "*** Provide your Access Key ***"
# sk = "*** Provide your Secret Key ***"
# region = " "


appName=""


def test():
    try:
        return disclient.disclient(endpoint=endpoint,ak=ak,sk=sk,projectid=projectid,region=region)
    except Exception as ex:
        print(str(ex))


#create your app
def createApp_test():
    cli = test()
    try:
        r=cli.createApp(appName=appName)
        print(r.statusCode)
    except Exception as ex:
        print(str(ex))


if __name__ == '__main__':
    print("start createApp")
    createApp_test()

