import pandas as pd



区县超限100明细= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\10月统计\结果表\月报\9月上报数据v3.0\10上报数据V6.0\【超限100%数据】10月地市超限100%数据.xlsx")
#地市编码表= pd.read_excel(r"G:\智诚\python代码\程序文件\案件处理程序表\t2_依赖的地市编码表\浙江地市编码表.xlsx")
# 案件采集状态= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件采集状态')
# 案件处罚结果= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件处罚结果')
# 原始车牌['过车时间'] = pd.to_datetime(原始车牌['过车时间'])
# 原始车牌['过车时间'] = 原始车牌['过车时间'].map(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
# 最终= pd.merge(初始数据,案件采集状态,on=['案件记录号'],how='left')
# 最终= pd.merge(最终,案件处罚结果,on=['案件记录号'],how='left')
区县超限100数=区县超限100明细.groupby(['区县','站点名称'])['序号'].count().reset_index(name='超限100数')
# 最终= pd.merge(区县超限100数,地市编码表,left_on=['区划代码'],right_on=['county_code'],how='left')

print(区县超限100数)
区县超限100数.to_excel(r"G:\智诚\日常给出数据汇总\月通报\10月统计\结果表\月报\9月上报数据v3.0\10上报数据V6.0\超限100%站点汇总.xlsx")
# print(最终.info())
# 车牌处理= pd.read_excel("C:/Users/stayhungary/Desktop/车牌处理.xlsx")

# 车牌处理['out_station_time'] = 车牌处理['out_station_time'].map(lambda x:x.strftime('%m/%d/%Y %H:%M:%S'))
# print(车牌处理.info())
# 最终= pd.merge(原始车牌,车牌处理,on=['car_no','out_station_time'],how='left')
#
#
#
# 最终.to_excel("C:Users/stayhungary/Desktop/车牌处理结果.xlsx")