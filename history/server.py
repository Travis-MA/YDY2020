#!/usr/bin/python
# -*- coding:utf-8 -*-
import Tools
import Factory
import Data

# new一个DIS数据工具
disDataTool = Factory.DISDataToolFactory().newObject('shardId-0000000000', 0, 'dis-YDY1')

# 新建一个空的蒸压釜实时记录数据
autoClaveRealTimeData = Data.AutoClaveRealTimeData()

# 利用DIS数据工具得到数据
autoClaveRealTimeData = disDataTool.getData(autoClaveRealTimeData)

print(autoClaveRealTimeData.getRecordList())



