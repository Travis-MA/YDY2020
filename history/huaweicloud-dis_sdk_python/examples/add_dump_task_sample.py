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


streamname = "dis-test2"
partitionCount=1
streamType="COMMON"
# dump_switch="obs_Periodic"/"MRS"/"DLI"/"CloudTable_HBase"/"CloudTable_OpenTSDB"/"DWS"/"text"/"parquet"
dump_switch="obs_Periodic"


def test():
    try:
        return disclient.disclient(endpoint,ak,sk,projectid,region)
    except Exception as ex:
        print(str(ex))

# create your streamname
def add_dump_task_test():
    cli = test()
    if dump_switch == "obs_Periodic":
        jsonBody = Dump_switch_obs_Periodic()
    elif dump_switch == "MRS":
        jsonBody = Dump_switch_MRS()
    elif dump_switch == "DLI":
        jsonBody = Dump_switch_DLI()
    elif dump_switch == "CloudTable_HBase":
        jsonBody = Dump_switch_CloudTable_HBase()
    elif dump_switch == "CloudTable_OpenTSDB":
        jsonBody = Dump_switch_CloudTable_OpenTSDB()
    elif dump_switch == "DWS":
        jsonBody = Dump_switch_DWS()
    elif dump_switch == "parquet":
        jsonBody = Dump_switch_JSON_parquet()
    else:
        jsonBody = Dump_switch_text()
    try:
        r = cli.add_dump_task(streamname, partitionCount, streamType, jsonBody=jsonBody)
        if r.statusCode==201:
            print('"%s" add dump task successfully !' % (streamname))

    except Exception as ex:
        print(str(ex))



def Dump_switch_obs_Periodic():
    jsonBody={
               'obs_destination_descriptor':{
                   "agency_name": "all",
                   "obs_bucket_path": "002",
                   "consumer_strategy": "LATEST",
                   "destination_file_type": 'text',
                   "source_data_type": "BLOB",
                   "task_name": "test_1",
                   "file_prefix": "",
                   "deliver_time_interval": 300,
                   "partition_format": "yyyy/MM/dd/HH/mm",
                   "record_delimiter": "\n"
                 }

        }
    return jsonBody


def Dump_switch_MRS():
    jsonBody={
                'mrs_destination_descriptor':{
                   "agency_name": "",
                    "mrs_cluster_id": "",
                    "mrs_cluster_name": "",
                    "consumer_strategy": "LATEST",
                    "mrs_hdfs_path": "",
                    "obs_bucket_path": "",
                    "file_prefix": "",
                    # deliver_time_interval=300~900
                    "deliver_time_interval": 300,
                    # retry_duration=0~7200
                    "retry_duration": 1800
                 }

        }
    return jsonBody

def Dump_switch_text():
    jsonBody ={
                 'obs_destination_descriptor':{
                 "agency_name": "all",
                 "consumer_strategy":"LATEST",
                 "obs_bucket_path": "002",
                 "file_prefix": "",
                 "deliver_time_interval": 300,
                 "partition_format": "yyyy/MM/dd/HH/mm",
                 "record_delimiter": '\n',
                 "destination_file_type":"text",
                 "source_data_type":"BLOB",
                 "task_name":"test1"
            }
         }
    return jsonBody

def Dump_switch_JSON_parquet():
    jsonBody ={
                 'obs_destination_descriptor':{
                 "agency_name": "",
                 "consumer_strategy": "LATEST",
                 "data_schema_path": "",
                 "obs_bucket_path": "",
                 "file_prefix": "",
                 "deliver_time_interval": 300,
                 "destination_file_type": "parquet",
                 "partition_format": "yyyy/MM/dd/HH/mm",
                 "record_delimiter": '\n',
                 "source_data_type": "JSON",
                 "task_name":""
            }
         }
    return jsonBody

def Dump_switch_DLI():
    jsonBody ={
                'dli_destination_descriptor':{
                "agency_name": "dws",
                "dli_database_name": "dis_dan",
                "dli_table_name": "cc",
                "obs_bucket_path": "002",
                "deliver_time_interval": 300,
                "retry_duration": 1800,
                "file_prefix":'',
                "consumer_strategy":'LATEST',
                "task_name":"test_1"
            }
         }
    return jsonBody

def Dump_switch_DWS():
    jsonBody ={
                'dws_destination_descriptor':{
                "agency_name":"",
                "consumer_strategy":"LATEST",
                "deliver_time_interval":300,
                "dws_cluster_id":"",
                "dws_cluster_name":"",
                "dws_database_name":"",
                "dws_delimiter":"\n",
                "dws_schema":"255x",
                "dws_table_name":"",
                "file_prefix":"",
                "kms_user_key_id":"",
                "kms_user_key_name":"",
                "obs_bucket_path":"",
                "retry_duration":1800,
                "source_data_type":"BLOB",
                "task_name":"",
                "user_name":"",
                "user_password":""
            }
         }
    return jsonBody


def Dump_switch_CloudTable_HBase():
    jsonBody ={
                'cloudtable_destination_descriptor':{
                    'backup_file_prefix':"",
                    'cloudtable_cluster_id':"",
                    'cloudtable_cluster_name':"",
                    'cloudtable_row_key_delimiter':"\n",
                    'cloudtable_schema':{
                            'row_key': [
                                {'value': "",
                                 'type': 'Bigint'}
                            ],
                            'columns': [
                                {'column_family_name': "",
                                 'column_name': "",
                                 'value': "",
                                 'type': "Bigint"}
                            ]
                        },
                    'cloudtable_table_name':"",
                    # 'consumer_strategy': "TRIM_HORIZON" or "LATEST"
                    'consumer_strategy':"LATEST",
                    'obs_backup_bucket_path':"",
                    'retry_duration':1800,
                    'source_data_type':"JSON",
                    'task_name':""
            }
         }
    return jsonBody


def Dump_switch_CloudTable_OpenTSDB():
    jsonBody ={
                'cloudtable_destination_descriptor':{
                    'backup_file_prefix': "",
                    'cloudtable_cluster_id':"d9b4fa51-a725-4093-bbdc-791ecd64a501" ,
                    'cloudtable_cluster_name': "cloudtable-cb14",
                    # 'consumer_strategy': "TRIM_HORIZON" or "LATEST"
                    'consumer_strategy': "LATEST",
                    'obs_backup_bucket_path':"dis",
                    'opentsdb_schema':[
                            {'metric': [
                                    {'type': "String",
                                     'value': "1"}
                                ],
                             'tags': [
                                     {'name': "3",
                                      'value': "4",
                                      'type': "Bigint"}
                             ],
                             'timestamp': {'value': "2", 'type': "String", 'format': "yyyy/MM/dd HH:mm:ss"},
                             'value': {'value': "3", 'type': "Bigint"}
                            }
                        ],
                    'retry_duration': 1800,
                    'source_data_type': "JSON",
                    'task_name': "test1"
            }
         }
    return jsonBody



if __name__ == '__main__':
    print("start add dump task")
    add_dump_task_test()
