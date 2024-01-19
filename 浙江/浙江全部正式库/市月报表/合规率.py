import pymysql
import pandas as pd
import time
from decimal import *
import numpy as np
import datetime as dt
import io

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


""" 引入原始表 """
sql = " select distinct site_name from t_bas_over_data_71"
f = get_df_from_db(sql)
print(f)

sql = " SELECT count(car_no) FROM t_bas_over_data_31 where out_station_time >= '2022-01-01 00:00:00' and  out_station_time <'2022-06-01 00:00:00'  and overrun_rate>=80  and area_county ='330482' and is_collect='1' "
a = get_df_from_db(sql)
print(a)

sql = "SELECT count(car_no) FROM t_bas_over_data_collection_31 a WHERE  a.`status` in (3,4,5,6,9,12,13) and a.out_station_time >= '2022-01-01 00:00:00' and a.out_station_time < '2022-06-01 00:00:00' and a.overrun_rate >80 and a.area_county ='330482'"
b = get_df_from_db(sql)
print(b)
"""
sql = "SELECT * FROM t_bas_pass_data_31 where out_station_time >= '2022-05-27 00:00:00' and  out_station_time <'2022-06-03 00:00:00' and out_station='3301833109' "
c = get_df_from_db(sql)


sql = "SELECT * FROM t_bas_pass_data_31 where out_station_time >= '2022-05-27 00:00:00' and  out_station_time <'2022-06-03 00:00:00' and out_station='3301833110' "
d= get_df_from_db(sql)

sql = "SELECT * FROM t_bas_pass_data_31 where out_station_time >= '2022-05-27 00:00:00' and  out_station_time <'2022-06-03 00:00:00' and out_station='330122_S302K16+450' "
e = get_df_from_db(sql)
print(e)

q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
with pd.ExcelWriter('{}/6个站点明细.xlsx'.format(q))as writer1:
     a.to_excel(writer1, sheet_name='蒋大线9K+300', index=True)
     b.to_excel(writer1, sheet_name='G320 281K+800', index=True)
     c.to_excel(writer1, sheet_name='S305 34K+700', index=True)
     d.to_excel(writer1, sheet_name='S302 14K+700', index=True)
     e.to_excel(writer1, sheet_name='S302（新淳线）淳安方向K16+500', index=True)
     f.to_excel(writer1, sheet_name='南新线17K+300', index=True)"""
"""
月超限80以上总数 = 月超限80以上总数['id_x']
print(月超限80以上总数)

U_过车_站点表 = U_过车_站点表[(U_过车_站点表['is_collect'] == 1)
]

月超限80以上且满足处罚条件总数 = U_过车_站点表.groupby(['city_code', 'city']).count()

月超限80以上且满足处罚条件总数 = 月超限80以上且满足处罚条件总数['id_x']

snum = [3, 4, 5, 6, 9, 12, 13]

U_过车_站点表 = U_案件审核_区域表[U_案件审核_区域表.loc[:, 'status_x'].isin(snum)]

月超限80以上审核通过总数 = U_过车_站点表.groupby(['city_code', 'city']).count()

月超限80以上审核通过总数 = 月超限80以上审核通过总数['id_x']

超限80数 = pd.merge(月超限80以上总数, 月超限80以上且满足处罚条件总数, on='city', how='left')

超限80数 = pd.merge(超限80数, 月超限80以上审核通过总数, on='city', how='left')

超限80数.rename(columns={'id_x_x': '月超限80%以上总数', 'id_x_y': '月超限80%以上且满足处罚条件总数', 'id_x': '月超限80%以上审核通过总数'}, inplace=True)
print(超限80数)

超限80数.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/处罚率，合规率.xlsx") """
"""U_过车_站点表=U_过车_站点表[(U_过车_站点表['is_collect']==1)
                        ]
print(U_过车_站点表)
月超限80以上且满足处罚条件总数=U_过车_站点表.groupby(['city_code','city']).count()
print(月超限80以上且满足处罚条件总数)

月超限80以上且满足处罚条件总数=月超限80以上且满足处罚条件总数['id_x']
print(月超限80以上且满足处罚条件总数)

snum=[3,4,5,6,9,12,13]

U_过车_站点表 = U_案件审核_区域表[U_案件审核_区域表.loc[:,'status_x'].isin(snum)]
U_过车_站点表 = U_过车_站点表[U_过车_站点表.loc[:,'out_station'].isin(station_code)]
print(U_过车_站点表)


月超限80以上审核通过总数=U_过车_站点表.groupby(['city_code','city']).count()
print(月超限80以上审核通过总数)

月超限80以上审核通过总数=月超限80以上审核通过总数['id_x']
print(月超限80以上审核通过总数)

超限80数 = pd.merge(月超限80以上总数,月超限80以上且满足处罚条件总数,on='city',how='left')
print(超限80数)
超限80数 = pd.merge(超限80数,月超限80以上审核通过总数,on='city',how='left')

超限80数.rename(columns={'id_x_x':'月超限80%以上总数','id_x_y':'月超限80%以上且满足处罚条件总数','id_x':'月超限80%以上审核通过总数'},inplace=True)
print(超限80数)
U_过车_站点表.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/U_过车_站点表.xlsx")

U_过车_站点表.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/U_过车_站点表.xlsx")"""

"""i=U_过车_站点表.loc[U_过车_站点表['area_city_x'] == '330100']

U_过车_站点表=U_过车_站点表[(U_过车_站点表['超限率100%']>=100)
                        ]
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
for item in i.items():
    key = item[0]
    value = item[1]
    print((key, value))
    U_过车_站点表.loc[((U_过车_站点表['area_county_x'] == key) | (U_过车_站点表['area_city_x'] == key)) & (U_过车_站点表['total_weight'] < 100 ), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(lambda x: float(x)*value).round(4)
U_过车_站点表['limit_weight']=U_过车_站点表['limit_weight'].astype('float')
U_过车_站点表['total_weight']=U_过车_站点表['total_weight'].astype('float')
U_过车_站点表['vehicle_brand']=U_过车_站点表['vehicle_brand'].astype('float')
U_过车_站点表['超限率100%']=U_过车_站点表.apply(lambda x:  (x['total_weight']-x['vehicle_brand'])*100/x['vehicle_brand'], axis= 1).round(2)
print(U_过车_站点表['limit_weight'].dtypes)



i['limit_weight']=i['limit_weight'].map(lambda x: x*11/10)
i['overate100']=i.apply(lambda x:  (x['total_weight']-x['limit_weight'])*100/x['limit_weight'], axis= 1)
print(i)"""

""" 
U_过车_站点表=U_过车_站点表[(U_过车_站点表['overrun_rate']>80)
                        ]

   超限80数 = pd.merge(月超限80以上总数,月超限80以上且满足处罚条件总数,left_index='city',right_index='city',how='left')
print(超限80数)                     

        4月超限80%以上总数=U_过车_站点表.groupby(['地市'])['站点名称'].count()                

U_过车_站点表=pd.DataFrame(U_过车_站点表,columns=["city","county","car_no","out_station_time","axis","total_weight","overrun","overrun_rate","site_name","status","is_collect","is_unusual","record_code","photo1","photo2","photo3","vedio"])

U_过车_站点表=U_过车_站点表.sort_values(by=['city','county']).reset_index(drop=True)
U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])


if (U_过车_站点表.loc[U_过车_站点表['area_city_x'] == '330100']):
        U_过车_站点表['limit_weight']=U_过车_站点表['limit_weight'].map(lambda x: x*1.1)
        print(U_过车_站点表['total_weight'].dtype())
U_过车_站点表['超限率100%']=(float(U_过车_站点表['total_weight'])-float(U_过车_站点表['vehicle_brand']))*100/float(U_过车_站点表['vehicle_brand'])

        print(U_过车_站点表)"""

""" 筛选= U_过车_站点表[(U_过车_站点表.area_city_x == key)
                        ]
    print(筛选)

U_过车_站点表.loc[U_过车_站点表['area_city_x'] == '330100']
站点设备完好率['调整后完好率']=站点设备完好率.apply(lambda x:  x['修正完好数']/x['接入数（修正后）'], axis= 1).round(4)"""

"""s = input("请输入截止月份(比如04)：")

wb["NeedModify"]=wb["NeedModify"].map({"Yes":1,"No":0})   #替换

area_county,area_city,
sql = "SELECT * FROM t_code_area"
t_code_area=get_df_from_db(sql)

overrate = (U_过车_站点表.total_weight - U_过车_站点表.limit_weight*value) * 100 / (U_过车_站点表.limit_weight * value)

sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '2022-01-01 00:00:00' and valid_time < '2022-{}-01 00:00:00'".format(s)
t_bas_over_data_collection_31=get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >=20220101 and insert_time <2022{}01".format(s)
t_bas_over_data_collection_makecopy=get_df_from_db(sql)

sql = "SELECT * FROM t_case_sign_result where close_case_time >= '2022-01-01 00:00:00' and close_case_time < '2022-{}-01 00:00:00'".format(s)
t_case_sign_result=get_df_from_db(sql)

sql = "SELECT * FROM t_sys_station "
t_sys_station=get_df_from_db(sql)

U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31,t_code_area,left_on='area_county',right_on='county_code',how='left')

U_案件处罚_区域表= pd.merge(t_case_sign_result,t_code_area,left_on='area_county',right_on='county_code',how='left')

U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy,t_code_area,left_on='area_county',right_on='county_code',how='left')

T_入库查询=U_检测点_区域表.loc[(U_检测点_区域表['law_judgment']=="1")
                           ]

T_现场处罚=U_案件处罚_区域表[(U_案件处罚_区域表.record_type==99)
                            &(U_案件处罚_区域表.insert_type==5)
                            &(U_案件处罚_区域表.area_province=='330000')
                            ]

T_非现场处罚=U_案件处罚_区域表[(U_案件处罚_区域表.record_type==31)
                            &(U_案件处罚_区域表.insert_type==1)
                            &(U_案件处罚_区域表.data_source==1)
                            &(U_案件处罚_区域表.case_type==1)
                            &(U_案件处罚_区域表.area_province=='330000')
                            ]

T_外省抄告=U_外省抄告_区域表

T_入库查询=T_入库查询.sort_values(['area_city'],ascending=False).reset_index(drop=True)
T_入库数 = T_入库查询.groupby([T_入库查询['area_city'],T_入库查询['city']]).count()
print(T_入库数.head())
T_入库数 = T_入库数.loc[:,['id_x']]
T_入库数.rename(columns={'id_x':'入库数(系统)'},inplace=True)
print(T_入库数.head())

T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
T_现场处罚计数 = T_现场处罚.groupby([T_现场处罚['city']])['CASE_NUM'].count()
T_现场处罚计数=T_现场处罚计数.to_frame()
T_现场处罚计数.rename(columns={'CASE_NUM':'现场处罚(系统)'},inplace=True)
print(T_现场处罚计数.head())


T_非现场处罚= T_非现场处罚.drop_duplicates(['CASE_NUM'])
T_非现场处罚计数 =T_非现场处罚.groupby([T_非现场处罚['city']])['CASE_NUM'].count()
T_非现场处罚计数=T_非现场处罚计数.to_frame()
T_非现场处罚计数.rename(columns={'CASE_NUM':'非现场处罚(系统)'},inplace=True)
print(T_非现场处罚计数.head())

T_外省抄告 = T_外省抄告.groupby([T_外省抄告['city']]).count()
T_外省抄告计数=T_外省抄告.loc[:,['id_x']]
T_外省抄告计数.rename(columns={'id_x':'外省抄告(系统)'},inplace=True)
print(T_外省抄告计数)


W_案件统计 = pd.merge(T_入库数,T_现场处罚计数,left_on='city',right_on='city',how='outer')
W_案件统计 = pd.merge(W_案件统计,T_非现场处罚计数,left_on='city',right_on='city',how='outer')
W_案件统计 = pd.merge(W_案件统计,T_外省抄告计数,left_on='city',right_on='city',how='outer')
W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
print(W_案件统计)

W_案件统计.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/2022年处罚案件情况统计.xlsx")"""