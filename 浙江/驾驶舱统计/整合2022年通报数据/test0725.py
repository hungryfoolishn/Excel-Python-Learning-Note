import datetime

now = datetime.datetime.now()
date = datetime.datetime.strftime(now - datetime.timedelta(days=1), '%Y%m%d')  # 获取日期
# 当前时间  必填
start_time = '{}'.format(date)
end_time = '{}'.format(date)
print(end_time)