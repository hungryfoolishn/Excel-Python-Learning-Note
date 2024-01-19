import pandas as pd



区县超限100明细= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\9月驾驶舱数据\案件明细.xlsx")
#地市编码表= pd.read_excel(r"G:\智诚\python代码\程序文件\案件处理程序表\t2_依赖的地市编码表\浙江地市编码表.xlsx")
# 案件采集状态= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件采集状态')
# 案件处罚结果= pd.read_excel("C:/Users/stayhungary/Desktop/杭州车牌未处理9668.xlsx",sheet_name='案件处罚结果')
# 原始车牌['过车时间'] = pd.to_datetime(原始车牌['过车时间'])
# 原始车牌['过车时间'] = 原始车牌['过车时间'].map(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
# 最终= pd.merge(初始数据,案件采集状态,on=['案件记录号'],how='left')
# 最终= pd.merge(最终,案件处罚结果,on=['案件记录号'],how='left')
区县超限100明细 = 区县超限100明细.fillna(0, inplace=False)
区县超限100明细['入库数2']=区县超限100明细['入库数']+区县超限100明细['交警非现场处罚数']

区县超限100明细['处罚总数']=区县超限100明细['交警非现场处罚数']+区县超限100明细['非现场处罚(系统)']
区县超限100明细['处罚率(不含抄告']=0


# 最终= pd.merge(区县超限100数,地市编码表,left_on=['区划代码'],right_on=['county_code'],how='left')

print(区县超限100明细)
区县超限100明细.to_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\9月驾驶舱数据\案件处罚率.xlsx")
# print(最终.info())
# 车牌处理= pd.read_excel("C:/Users/stayhungary/Desktop/车牌处理.xlsx")

# 车牌处理['out_station_time'] = 车牌处理['out_station_time'].map(lambda x:x.strftime('%m/%d/%Y %H:%M:%S'))
# print(车牌处理.info())
# 最终= pd.merge(原始车牌,车牌处理,on=['car_no','out_station_time'],how='left')
#
#
#
# 最终.to_excel("C:Users/stayhungary/Desktop/车牌处理结果.xlsx")