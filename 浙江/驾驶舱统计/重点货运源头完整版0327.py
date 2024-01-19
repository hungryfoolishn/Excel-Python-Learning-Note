import pymysql
import pandas as pd
import numpy as np
import  time


db = pymysql.connect(
    host='172.19.116.150',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=11806,
    charset='utf8',
    database='db_manage_overruns'
    )


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


start_time='2022-12-01'
end_time ='2023-01-01'
""" 引入原始表 """

sql = "SELECT city,county,city_code,county_code  FROM t_code_area where province_code = '330000'"
t_code_area=get_df_from_db(sql)
# sql = "SELECT * from t_bas_source_company where area_province = '330000' and is_statictis =1  and is_deleted=0 "
# t_bas_source_company=get_df_from_db(sql)
# sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
# t_bas_source_company_equipment=get_df_from_db(sql)
站点总数 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\拉出表\重点货运源头445家明细0309.xlsx")
数据站点总数=站点总数.groupby(['city','city_code','county','county_code'])['id'].count().reset_index(name='数据站点总数')

数据站点总数['county_code']=数据站点总数['county_code'].astype('string')

sql = "SELECT * FROM t_sys_station WHERE station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null "
t_sys_station=get_df_from_db(sql)
sql = "SELECT area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate,axis" \
      " FROM t_bas_pass_data_71  where out_station_time >='{} 00:00:00' and out_station_time <='{} 00:00:00'  and is_truck =1 and insert_time <='{} 00:00:00' ".format(start_time,end_time,end_time)
t_bas_pass_data_71 =get_df_from_db(sql)




"""拼接表"""
U_源头_区域表 = pd.merge(t_bas_pass_data_71 ,t_code_area,left_on='area_county',right_on='county_code',how='left')

企业_源头_站点表 = pd.merge(t_sys_station,t_code_area,left_on='area_county',right_on='county_code',how='left')




station_code = 企业_源头_站点表['station_code']
U_源头_区域表= U_源头_区域表[U_源头_区域表.loc[:, 'out_station'].isin(station_code)]
# q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
# with pd.ExcelWriter('{}/U_源头_区域表.xlsx'.format(q))as writer1:
#      U_源头_区域表.to_excel(writer1, sheet_name='sheet1', index=True)



"""超限数"""
超限20_50 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 20) &(U_源头_区域表['overrun_rate'] <= 50)].groupby(['city','county','county_code'])['car_no'].count().reset_index(name='20-50%数')
超限50_100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 50) &(U_源头_区域表['overrun_rate'] <= 100)].groupby(['city','county','county_code'])['car_no'].count().reset_index(name='50-100%数')
超限100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 100)  &(U_源头_区域表['overrun_rate'] <= 450)].groupby(['city','county','county_code'])['car_no'].count().reset_index(name='100%以上数')




"""设备上线率"""

U_源头_区域表['取日']=U_源头_区域表['out_station_time'].apply(lambda x : x.strftime('%d'))
在线天数大于20天=U_源头_区域表.groupby(['city','county_code','county','out_station'])['取日'].nunique().reset_index(name='在线天数')
货运量大于2万吨=U_源头_区域表.groupby(['city','county_code','county','out_station'])['total_weight'].sum().reset_index(name='货运总重')
过车数大于410辆=U_源头_区域表.groupby(['city','county_code','county','out_station'])['city'].count().reset_index(name='货车数')
站点完好数 = pd.merge(在线天数大于20天,货运量大于2万吨,on=['city','county_code','county','out_station'],how='left')
站点完好数 = pd.merge(站点完好数,过车数大于410辆,on=['city','county_code','county','out_station'],how='left')
站点完好数['货运总重'] = pd.to_numeric(站点完好数['货运总重'],errors='coerce')


站点完好数区县=站点完好数.groupby(['city','county','county_code'])['货车数','货运总重'].sum()
在线站点数=站点完好数[(站点完好数['在线天数'] > 20) | (站点完好数['货运总重'] > 20000) | (站点完好数['货车数'] > 410)].groupby(['city','county','county_code'])['out_station'].count().reset_index(name='在线站点数')




"""聚合"""

货运源头监控数据=pd.merge(数据站点总数,超限20_50,on=['city','county','county_code'],how='left')
货运源头监控数据=pd.merge(货运源头监控数据,超限50_100 ,on=['city','county','county_code'],how='left')
货运源头监控数据=pd.merge(货运源头监控数据,超限100,on=['city','county','county_code'],how='left')
货运源头监控数据=pd.merge(货运源头监控数据,在线站点数,on=['city','county','county_code'],how='left')
货运源头监控数据=pd.merge(货运源头监控数据,站点完好数区县,on=['city','county','county_code'],how='left')
货运源头监控数据 =货运源头监控数据.fillna(0, inplace=False)
货运源头监控数据['源头单位平均过车数（辆次）'] = 货运源头监控数据.apply(lambda x: x['货车数']/(x['数据站点总数']+0.0000001) , axis=1).round(0)
货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据['20-50%占比'] = 货运源头监控数据.apply(lambda x: x['20-50%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据['50-100%占比'] = 货运源头监控数据.apply(lambda x: x['50-100%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据['100%以上占比'] = 货运源头监控数据.apply(lambda x: x['100%以上数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据 =货运源头监控数据.fillna(0, inplace=False)
货运源头监控数据['设备上线率（%）'] = 货运源头监控数据['设备上线率（%）'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据['20-50%占比'] = 货运源头监控数据['20-50%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据['50-100%占比'] = 货运源头监控数据['50-100%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据['100%以上占比'] = 货运源头监控数据['100%以上占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据.rename(columns={'city': '地市','county':'区县'}, inplace=True)
货运源头监控数据 = pd.DataFrame(货运源头监控数据, columns=['地市','区县','数据站点总数','货车数','源头单位平均过车数（辆次）','在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比','city_code','county_code'])
货运源头监控数据 = 货运源头监控数据.sort_values('county_code', ascending=True)

货运源头监控数据地市=货运源头监控数据.groupby(['地市','city_code']).sum().reset_index()
货运源头监控数据地市 =货运源头监控数据地市.fillna(0, inplace=False)
货运源头监控数据地市['源头单位平均过车数（辆次）'] = 货运源头监控数据地市.apply(lambda x: x['货车数']/(x['数据站点总数']+0.0000001) , axis=1).round(0)
货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市.apply(lambda x: x['20-50%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市.apply(lambda x: x['50-100%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市.apply(lambda x: x['100%以上数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据地市 =货运源头监控数据地市.fillna(0, inplace=False)
货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市['设备上线率（%）'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市['20-50%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市['50-100%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市['100%以上占比'] .apply(lambda x: format(x, '.2%'))

货运源头监控数据地市 = pd.DataFrame(货运源头监控数据地市, columns=['地市','数据站点总数','货车数','源头单位平均过车数（辆次）','在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比','city_code'])
货运源头监控数据地市 = 货运源头监控数据地市.sort_values('city_code', ascending=True)
货运源头监控数据省=货运源头监控数据地市
货运源头监控数据省['省'] = '浙江省'
货运源头监控数据省=货运源头监控数据省.groupby(['省']).sum().reset_index()
货运源头监控数据省 =货运源头监控数据省.fillna(0, inplace=False)
货运源头监控数据省['源头单位平均过车数（辆次）'] = 货运源头监控数据省.apply(lambda x: x['货车数']/(x['数据站点总数']+0.0000001) , axis=1).round(0)
货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据省['20-50%占比'] = 货运源头监控数据省.apply(lambda x: x['20-50%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据省['50-100%占比'] = 货运源头监控数据省.apply(lambda x: x['50-100%数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据省['100%以上占比'] = 货运源头监控数据省.apply(lambda x: x['100%以上数']/(x['货车数']+0.0000001) , axis=1)
货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数']/(x['数据站点总数']) , axis=1)
货运源头监控数据省 =货运源头监控数据省.fillna(0, inplace=False)
货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省['设备上线率（%）'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据省['20-50%占比'] = 货运源头监控数据省['20-50%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据省['50-100%占比'] = 货运源头监控数据省['50-100%占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据省['100%以上占比'] = 货运源头监控数据省['100%以上占比'] .apply(lambda x: format(x, '.2%'))
货运源头监控数据省.rename(columns={'省': '地市'}, inplace=True)
货运源头监控数据省 = pd.DataFrame(货运源头监控数据省, columns=['地市','数据站点总数','货车数','源头单位平均过车数（辆次）','在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比'])
货运源头监控数据省市 = pd.concat([货运源头监控数据地市, 货运源头监控数据省])


with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\全部源头.xlsx')as writer1:
    货运源头监控数据.to_excel(writer1, sheet_name='区县', index=False)
    货运源头监控数据地市.to_excel(writer1, sheet_name='地市', index=False)
    货运源头监控数据省市.to_excel(writer1, sheet_name='省', index=False)
    站点完好数.to_excel(writer1, sheet_name='站点数据明细', index=False)

"""货运源头监控数据.to_excel("C:/Users/Administrator/Desktop/日常报表/货运源头监控数据.xlsx")"""

"""货运源头监控数据=pd.merge(货运源头监控数据,各市站点数 ,left_on='区县',right_on='county',how='left')"""