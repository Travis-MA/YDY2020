from datetime import datetime
from datetime import timezone
from datetime import timedelta
folderPath = 'Service/ZyRecord/'
hourOffset = 7
today = datetime.today()
nowTime = datetime(year=today.year, month=today.month, day=today.day, hour=8, minute=0, second=0, microsecond=0,tzinfo=timezone(timedelta(hours=hourOffset)), fold=0)
offSetTime = nowTime - nowTime.tzinfo.utcoffset(nowTime)  # 减去7个小时的时间（今天上午七点前是昨天）
offSetHour = int(nowTime.tzinfo.utcoffset(nowTime).total_seconds() / 3600)
todayInitial = datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day, hour=offSetHour, minute=0, second=0)
todayFolderPath = folderPath + offSetTime.date().isoformat() + '/'

print('nowTime'+str(nowTime))
print('offSetTime'+str(offSetTime))
