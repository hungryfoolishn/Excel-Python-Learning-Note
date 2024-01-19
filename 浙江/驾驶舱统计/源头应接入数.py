import pandas as pd



区县超限100明细= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\9月驾驶舱数据\区县应接入数.xlsx")
地市编码表= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\9月驾驶舱数据\区域编码.xlsx")
# 案件采集状态= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件采集状态')
# 案件处罚结果= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件处罚结果')
# 原始车牌['过车时间'] = pd.to_datetime(原始车牌['过车时间'])
# 原始车牌['过车时间'] = 原始车牌['过车时间'].map(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
# 最终= pd.merge(初始数据,案件采集状态,on=['案件记录号'],how='left')
# 最终= pd.merge(最终,案件处罚结果,on=['案件记录号'],how='left')
# index = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
# columns = ['name', 'class_1', 'class_2', 'num']
# df = pd.DataFrame(data=data, index=index, columns=columns)

# 区县超限100数=区县超限100明细.groupby(['地市','区县']).agg({'货车流量': ['sum'],'超限货车': ['sum'],'超限<=20%车辆数': ['sum'],'20%<超限<=50%车辆数': ['sum'],'50%超限<=100%车辆数': ['sum'],'超限>100%车辆数': ['sum']})
区县超限100明细 = 区县超限100明细.fillna(value=0)


最终= pd.merge(区县超限100明细,地市编码表,left_on=['行政区划'],right_on=['area_name'],how='left')

print(最终)
最终.to_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\9月驾驶舱数据\货运源头区县应接入数.xlsx")
# print(最终.info())
# 车牌处理= pd.read_excel("C:/Users/stayhungary/Desktop/车牌处理.xlsx")

# 车牌处理['out_station_time'] = 车牌处理['out_station_time'].map(lambda x:x.strftime('%m/%d/%Y %H:%M:%S'))
# print(车牌处理.info())
# 最终= pd.merge(原始车牌,车牌处理,on=['car_no','out_station_time'],how='left')
#
#
#
# 最终.to_excel("C:Users/stayhungary/Desktop/车牌处理结果.xlsx")