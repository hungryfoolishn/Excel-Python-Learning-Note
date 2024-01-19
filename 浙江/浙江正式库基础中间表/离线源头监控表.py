import pymysql
import pandas as pd
import numpy as np
import  time


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


""" 引入原始表 """

# sql = "SELECT * FROM t_code_area"
# t_code_area=get_df_from_db(sql)
# sql = "SELECT * from t_bas_source_company where area_province = '330000'   "
# t_bas_source_company=get_df_from_db(sql)
# sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
# t_bas_source_company_equipment=get_df_from_db(sql)
# sql = "SELECT * FROM t_sys_station WHERE station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null "
# t_sys_station=get_df_from_db(sql)
# sql = "SELECT * FROM t_bas_pass_data_71  where out_station_time >='2022-09-01' and out_station_time <='2022-10-01' and area_province  = '330000' and is_truck =1 and insert_time <='2022-10-01' "
# t_bas_pass_data_71 =get_df_from_db(sql)
t_code_area = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_code_area.xls")
t_bas_source_company = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_source_company.xls")
t_bas_source_company_equipment = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_source_company_equipment.xls")
t_sys_station = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_sys_station.xls")
t_bas_pass_data_711 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_pass_data_711.xls")
t_bas_pass_data_712 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_pass_data_713.xls")
t_bas_pass_data_713 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_bas_pass_data_714.xls")
t_bas_pass_data_71=pd.concat([t_bas_pass_data_711,t_bas_pass_data_712])
print(t_bas_pass_data_71)
t_bas_pass_data_71=pd.concat([t_bas_pass_data_71,t_bas_pass_data_713])
print(t_bas_pass_data_71)
站点总数 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\重点货运源头接入数模板.xlsx")

"""拼接表"""
U_源头_区域表 = pd.merge(t_bas_pass_data_71 ,t_code_area,left_on='area_county',right_on='county_code',how='left')
企业_源头表= pd.merge(t_bas_source_company,t_bas_source_company_equipment,left_on='id',right_on='source_company_id',how='left')
企业_源头表['station_code']=企业_源头表['station_code'].astype('string')
企业_源头_站点表 = pd.merge(企业_源头表,t_sys_station,left_on='station_code',right_on='station_code',how='left')
# 企业_源头_站点表.to_excel("G:/智诚/日常给出数据汇总/月通报/9月统计/企业_源头_站点表2.xlsx")
数据站点总数=企业_源头_站点表.groupby(['area_city_y'])['id_x'].count()
数据站点总数 = 数据站点总数.to_frame()
数据站点总数= 数据站点总数.reset_index()
print(数据站点总数)

station_code = 企业_源头_站点表['station_code']
U_源头_区域表= U_源头_区域表[U_源头_区域表.loc[:, 'out_station'].isin(station_code)]


过车数 = U_源头_区域表.groupby(['city_code','city'])['id_x'].count()
过车数 = 过车数.to_frame()
过车数= 过车数.reset_index()
print(过车数)



"""超限数"""
U_源头_区域表.describe()
# U_源头_区域表.to_excel("G:/智诚/日常给出数据汇总/月通报/9月统计/U_源头_区域表.xlsx")

超限20_50 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 20) &(U_源头_区域表['overrun_rate'] <= 50)].groupby(['city_code','city','county_code','county'])['car_no'].count()
超限20_50 = 超限20_50.to_frame()
超限20_50 = 超限20_50.reset_index()
超限50_100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 50) &(U_源头_区域表['overrun_rate'] <= 100)].groupby(['city_code','city'])['car_no'].count()
超限50_100 = 超限50_100.to_frame()
超限50_100 = 超限50_100.reset_index()
超限100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 100)  &(U_源头_区域表['overrun_rate'] <= 450)].groupby(['city_code','city'])['car_no'].count()
print(超限100)
超限100 = 超限100.to_frame()
超限100 = 超限100.reset_index()




"""设备上线率"""
U_源头_区域表['取日']=U_源头_区域表['out_station_time'].apply(lambda x : x.strftime('%d'))
在线天数大于20天=U_源头_区域表.groupby(['city_code','county_code','out_station'])['取日'].nunique()
在线天数大于20天 = 在线天数大于20天.to_frame()
在线天数大于20天= 在线天数大于20天.reset_index()
货运量大于2万吨=U_源头_区域表.groupby(['city_code','county_code','out_station'])['total_weight'].sum()
货运量大于2万吨 =货运量大于2万吨.to_frame()
货运量大于2万吨= 货运量大于2万吨.reset_index()
过车数大于410辆=U_源头_区域表.groupby(['city_code','county_code','out_station'])['id_x'].count()
过车数大于410辆 =过车数大于410辆.to_frame()
过车数大于410辆= 过车数大于410辆.reset_index()
站点完好数 = pd.merge(在线天数大于20天,货运量大于2万吨,left_on='out_station',right_on='out_station',how='left')
站点完好数 = pd.merge(站点完好数,过车数大于410辆,left_on='out_station',right_on='out_station',how='left')
# 站点完好数.to_excel("G:/智诚/日常给出数据汇总/月通报/9月统计/站点完好数.xlsx")
各市站点数=站点完好数[(站点完好数['取日'] > 20) | (站点完好数['total_weight'] > 2000) | (站点完好数['id_x'] > 410)].groupby(['city_code','county_code'])['out_station'].count()
各市站点数 = 各市站点数.to_frame()
各市站点数= 各市站点数.reset_index()
# 各市站点数=各市站点数.to_excel("G:/智诚/日常给出数据汇总/月通报/9月统计/源头区县站点完好数22.xlsx")

# """聚合"""
# 货运源头监控数据=pd.merge(站点总数,过车数,left_on='地市',right_on='city',how='left')
# 货运源头监控数据=pd.merge(货运源头监控数据,数据站点总数,left_on='city_code',right_on='area_city_y',how='left')
# 货运源头监控数据=pd.merge(货运源头监控数据,超限20_50,left_on='地市',right_on='city',how='left')
# 货运源头监控数据=pd.merge(货运源头监控数据,超限50_100 ,left_on='地市',right_on='city',how='left')
# 货运源头监控数据.rename(columns={'id_x_x': '货车数','id_x_y':'数据站点总数','car_no_x': '20-50%','car_no_y': '50-100%',}, inplace=True)
# 货运源头监控数据 = pd.DataFrame(货运源头监控数据, columns=['地市', '站点总数', '数据站点总数','货车数','20-50%','50-100%'])
# 货运源头监控数据=pd.merge(货运源头监控数据,超限100,left_on='地市',right_on='city',how='left')
# 货运源头监控数据=pd.merge(货运源头监控数据,各市站点数,left_on='地市',right_on='city',how='left')
# 货运源头监控数据.rename(columns={'out_station': '各市站点数','car_no':'100%以上'}, inplace=True)
# 货运源头监控数据['实现互联网接入比率(%)']=货运源头监控数据['数据站点总数']/货运源头监控数据['站点总数']
# 货运源头监控数据 =货运源头监控数据.fillna(0, inplace=False)
# 货运源头监控数据['实现互联网接入比率(%)'] = 货运源头监控数据['实现互联网接入比率(%)'].apply(lambda x: format(x, '.2%'))
# 货运源头监控数据['源头单位平均过车数（辆次）'] = 货运源头监控数据.apply(lambda x:x['货车数']/(x['数据站点总数']+0.001) , axis=1).round(2)
# 货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['各市站点数']/(x['数据站点总数']+0.001) , axis=1).round(2)
# 货运源头监控数据 =货运源头监控数据.fillna(0, inplace=False)
# 货运源头监控数据['设备上线率（%）'] = 货运源头监控数据['设备上线率（%）'] .apply(lambda x: format(x, '.2%'))
# 货运源头监控数据 = pd.DataFrame(货运源头监控数据, columns=['地市', '站点总数', '数据站点总数','实现互联网接入比率(%)','货车数','源头单位平均过车数（辆次）','各市站点数','设备上线率（%）','20-50%','50-100%','100%以上'])
# print(货运源头监控数据)
#
# q = input("请输入储存路径(G:/智诚/日常给出数据汇总/月通报/9月统计/货运源头数据)：")
# with pd.ExcelWriter('{}/源头2.xlsx'.format(q))as writer1:
#      货运源头监控数据.to_excel(writer1, sheet_name='sheet1', index=True)
"""货运源头监控数据.to_excel("C:/Users/Administrator/Desktop/日常报表/货运源头监控数据.xlsx")"""

"""货运源头监控数据=pd.merge(货运源头监控数据,各市站点数 ,left_on='地市',right_on='city',how='left')"""