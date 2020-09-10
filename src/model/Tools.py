#!/usr/bin/python
# -*- coding:utf-8 -*-


import abc
import json
import os
import sys

#工具抽象类
class Tools(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def getType(self):
        pass


#数据工具
class DataTool(Tools, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setConfPath(self):
        pass

    @abc.abstractmethod
    def getData(self):
        pass

    @abc.abstractmethod
    def postData(self, data):
        pass

