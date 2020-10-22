

# !/usr/bin/python
# -*- coding:utf-8 -*-
import sys

from src.Task import DIStoOBSscheduleTask

# 5分钟一次

disToObsSchedule = DIStoOBSscheduleTask(10)
disToObsSchedule.run(1)


