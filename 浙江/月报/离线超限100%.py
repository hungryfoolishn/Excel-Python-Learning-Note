import pymysql
import pandas as pd
import  time
from decimal import *
import numpy as np
import datetime as dt
import io

# db = pymysql.connect(
#     host='192.168.2.119',
#     user='zjzhzcuser',
#     passwd='F4dus0ee',
#     port=3306,
#     charset='utf8',
#     database='db_manage_overruns'
#     )


def get_df_from_db(sql):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """
    return df

'''输入变量'''
cs = input("请输入省市名：")



""" 引入原始表 """
start = time.time()
# t_code_area = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_code_area.xls")
# t_bas_source_company = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_source_company.xls")
# t_bas_source_company_equipment = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_source_company_equipment.xls")
# t_sys_station = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_sys_station.xls")
# t_bas_pass_data_71 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_pass_data_71.xls")


df_数据汇总= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\9月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=False)
print(df_数据汇总)
# sql = "SELECT * FROM t_sys_station"
# t_sys_station=get_df_from_db(sql)
t_sys_station = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\t_sys_station.xls")
U_汇总_站点表 = pd.merge(df_数据汇总,t_sys_station,left_on='站点名称',right_on='station_name',how='left')
station_code=U_汇总_站点表['station_code']
# sql = "SELECT * FROM t_code_area  "
# t_code_area=get_df_from_db(sql)
t_code_area = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_code_area.xls")
# sql = "SELECT * FROM t_bas_over_data_31 where is_unusual = 0 and out_station_time >= '2022-04-01 00:00:00' and  out_station_time <'2022-05-01 00:00:00' and allow is null  and is_unusual=0  "
# t_bas_over_data_31=get_df_from_db(sql)
t_bas_over_data_31= pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\t_bas_over_data_31.xls")
print(t_bas_over_data_31)
end = time.time()
time = end - start
print(time)


"""超限1000全省明细"""

U_过车_区域表 = pd.merge(t_bas_over_data_31,t_code_area,left_on='area_county',right_on='county_code',how='left')
print(U_过车_区域表)
U_过车_站点表 = pd.merge(U_过车_区域表,t_sys_station,left_on='out_station',right_on='station_code',how='left')
print(U_过车_站点表)


U_过车_站点表= U_过车_站点表[U_过车_站点表.loc[:,'out_station'].isin(station_code)]


i = {
    "330100": 1.1,
    "330200": 1,
    "330300": 1,
    "330400": 1.1,
    "330500": 1,
    "330600": 1,
    "330700": 1,
    "330800": 1.1,
    "330900": 1.1,
    "331000": 1.1,
    "331100": 1.1,
    "330122": 1.2,
    "330183": 1.21,
    "330523": 1.03,
    "330603": 1.1,
    "330604": 1.2,
    "330624": 1.2,
    "330681": 1.1,
    "330703": 1.2,
    "330782": 1.1
    }
U_过车_站点表['area_county_x']=U_过车_站点表['area_county_x'].astype('string')
U_过车_站点表['area_city_x']=U_过车_站点表['area_city_x'].astype('string')
for item in i.items():
    key = item[0]
    value = item[1]
    print((key, value))
    U_过车_站点表.loc[((U_过车_站点表['area_county_x'] == key) | (U_过车_站点表['area_city_x'] == key)) & (U_过车_站点表['total_weight'] < 100 ), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(lambda x: float(x)*value).round(4)
print(U_过车_站点表['vehicle_brand'])


U_过车_站点表['limit_weight']=U_过车_站点表['limit_weight'].astype('float')
U_过车_站点表['total_weight']=U_过车_站点表['total_weight'].astype('float')
U_过车_站点表['vehicle_brand']=U_过车_站点表['vehicle_brand'].astype('float')

U_过车_站点表['超限率100%']=U_过车_站点表.apply(lambda x:  (x['total_weight']-x['vehicle_brand'])*100/x['vehicle_brand'], axis= 1).round(2)

U_过车_站点表=U_过车_站点表[(U_过车_站点表['超限率100%']>=100)
                        ]

print(U_过车_站点表)
with pd.ExcelWriter(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\U_过车_站点表2.xlsx") as writer1:
    U_过车_站点表.to_excel(writer1, sheet_name='超限100%数明细表', index=True)
U_过车_站点表=pd.DataFrame(U_过车_站点表,columns=["city","county","car_no","out_station_time","axis","total_weight","overrun","overrun_rate","site_name","status_x","is_collect","is_unusual","record_code","photo1","photo2","photo3","vedio"])

U_过车_站点表=U_过车_站点表.sort_values(by=['city','county','out_station_time'],ascending=False).reset_index(drop=True)
U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])
U_过车_站点表.loc[U_过车_站点表['status_x'] == 1, 'status_x'] = '待初审'
U_过车_站点表.loc[U_过车_站点表['status_x'] == 2, 'status_x'] = '待审核'
U_过车_站点表.loc[U_过车_站点表['status_x'] == 9, 'status_x'] = '判定不处理'
U_过车_站点表.loc[U_过车_站点表['status_x'] == 15, 'status_x'] = '初审不通过'
U_过车_站点表.loc[U_过车_站点表['is_collect'] == 1, 'is_collect'] = '满足'
U_过车_站点表.loc[U_过车_站点表['is_collect'] == 0, 'is_collect'] = '不满足'
print(U_过车_站点表)


"""浙江超限100%明细数据 以及地市货车数及超限100%数"""
if cs=='浙江' :

    df_接入数 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\接入数.xlsx")
    df_数据汇总 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\9月数据汇总表.xls")
    df_数据汇总.columns = df_数据汇总.iloc[2]
    df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
    df_报修点位统计 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\报修点位统计表.xls")
    df_报修点位统计.columns = df_报修点位统计.iloc[0]
    df_报修点位统计 = df_报修点位统计.iloc[1:].reset_index(drop=True)
    货车数 = df_数据汇总.groupby(['地市'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数= 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['city'])['record_code'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100,left_on='地市',right_on='city', how='left', )
    附件2.rename(columns={'record_code': '超限100%数'}, inplace=True)
    附件2 = 附件2.fillna(0, inplace=False)
    附件2['超限100%数占货车数比例'] = 附件2.apply(lambda x: x['超限100%数']/x['货车数'], axis=1)
    附件2['超限100%数占货车数比例'] = 附件2['超限100%数占货车数比例'].apply(lambda x: format(x, '.3%'))
    附件2['排名（占比由高到低）'] = 附件2['超限100%数占货车数比例'] .rank(ascending=True, method='first')
    附件2 = pd.merge(df_接入数, 附件2, on='地市', how='left')
    附件2 = pd.DataFrame(附件2, columns=['地市', '货车数', '超限100%数','超限100%数占货车数比例','排名（占比由高到低）'])
    附件2 =附件2.drop(index=(附件2.loc[(附件2['地市'] == '义乌')].index))
    print(附件2)
    with pd.ExcelWriter(r"G:\智诚\日常给出数据汇总\月通报\9月统计\月报\结果表\超限100%\XX月地市超限100%数据.xlsx")as writer1:
        附件2.to_excel(writer1, sheet_name='地市货车数及超限100%数', index=True)
        U_过车_站点表.to_excel(writer1, sheet_name='超限100%数明细表', index=True)




elif cs=='杭州':
    """杭州公路超限100%车辆占货车数量比例情况表"""
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    U_过车_站点表=U_过车_站点表[(U_过车_站点表.city == '{}'.format(cs))]
    df_区县排列 = pd.read_excel("C:/Users/Administrator/Desktop/输出报表/{}市月报表/{}市区县模板.xlsx".format(cs, cs))
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数 = 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['county'])['record_code'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100, left_on='区县', right_on='county', how='left' )
    附件2.rename(columns={'record_code': '超限100%数'}, inplace=True)
    附件2 = 附件2.fillna(0, inplace=False)
    附件2['占比数(超限数/万辆）'] = 附件2.apply(lambda x: x['超限100%数'] / x['货车数'] * 10000, axis=1).round(2)
    附件2 = pd.merge(df_区县排列, 附件2, on='区县', how='left')
    附件2 = pd.DataFrame(附件2, columns=['区县', '货车数', '超限100%数', '占比数(超限数/万辆）'])
    print(附件2)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/杭州市月报表/杭州市公路超限100%车辆占货车数量比例情况表.xlsx')as writer1:
        附件2.to_excel(writer1, sheet_name='公路超限100%车辆占货车数量比例情况表', index=True)


elif cs == '湖州':
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表.city == '{}'.format(cs))]
    df_区县排列 = pd.read_excel("C:/Users/Administrator/Desktop/输出报表/{}市月报表/{}市区县模板.xlsx".format(cs, cs))
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数 = 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['county'])['record_code'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100, left_on='区县', right_on='county', how='left')
    附件2.rename(columns={'record_code': '超限100%数'}, inplace=True)
    附件2['超限100%数货车数/万辆'] = 附件2.apply(lambda x: x['超限100%数'] / x['货车数'] * 10000, axis=1).round(2)
    附件2 = pd.merge(df_区县排列, 附件2, on='区县', how='left')
    附件2 = pd.DataFrame(附件2, columns=['区县', '货车数', '超限100%数', '超限100%数货车数/万辆'])
    附件2 = 附件2.fillna(0, inplace=False)
    print(附件2)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/湖州市月报表/湖州市公路超限100%车辆占货车数量比例情况表.xlsx')as writer1:
        附件2.to_excel(writer1, sheet_name='公路超限100%车辆占货车数量比例情况表', index=True)

else:
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表.city == '{}'.format(cs))]
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数 = 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['county'])['record_code'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100, left_on='区县', right_on='county', how='left')
    附件2.rename(columns={'record_code': '超限100%数'}, inplace=True)
    附件2['超限100%数货车数/万辆'] = 附件2.apply(lambda x: x['超限100%数'] / x['货车数'] * 10000, axis=1).round(2)
    附件2 = pd.DataFrame(附件2, columns=['区县', '货车数', '超限100%数', '超限100%数货车数/万辆'])
    附件2 = 附件2.fillna(0, inplace=False)
    print(附件2)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/其他市月报表/{}市公路超限100%车辆占货车数量比例情况表.xlsx'.format(cs))as writer1:
        附件2.to_excel(writer1, sheet_name='公路超限100%车辆占货车数量比例情况表', index=True)

