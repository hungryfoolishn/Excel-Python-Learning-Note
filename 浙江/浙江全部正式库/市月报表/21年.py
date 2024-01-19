import pymysql
import pandas as pd


db = pymysql.connect(
    host='192.168.2.119',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=3306,
    charset='utf8',
    database='db_manage_overruns'
)

import pandas as pd
import pymysql
from datetime import datetime

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
aa=['安吉','德清','黄岩','柯桥','临安','龙湾','瑞安','诸暨']
#aa=['安吉']
sc_dz=r"C:\Users\Administrator\Desktop\9区县重点货运20220530-0605"
qishi_date ='2022-05-30'
jiezi_date ='2022-06-06'
qishi_time = datetime.strptime(qishi_date, '%Y-%m-%d')
jiezi_time = datetime.strptime(jiezi_date, '%Y-%m-%d')

#qishi_time=
#jiezi_time=
#aa=['安吉']
log=[]
#aa=['海宁']

for i in aa:
    sql3="select *\
        from t_bas_pass_data_71 as a\
        left join t_code_area as b\
        on a.area_county=b.county_code where county='{}'".format(i)

    sql = "select out_station_time,site_name,overrun_rate,is_truck,county\
        from t_bas_pass_data_71 as a\
        left join t_code_area as b\
        on a.area_county=b.county_code where county= %s and is_truck='1' and out_station_time >= %s and out_station_time <=%s"

    cursor = db.cursor()
    # 以下为传递多个参数的用法
    #cursor.execute(sql, [i,'2018-03-07 22:35:46','2022-06-07 22:35:46'])
    cursor.execute(sql, [i, qishi_time, jiezi_time])
    # 传递单个参数时 cursor.execute(sql,'B00140N5CS')
    print('bbbbbbbbbbbbbbbbb')
    data=cursor.fetchall()
    #print(cursor.fetchall())
    dq = pd.DataFrame(list(data))
    print(len(dq))
    #dq.columns=['out_station_time','site_name','overrun_rate','is_truck','county']
    #print(len(list(dq[0])))
    #[5,14]
    #db.close()
    if dq.empty:
        ll=i+"没读取到数据"
        log.append(ll)
        pass
#and out_station_time >= '2021-01-01 00:00:00' and out_station_time <='2022-05-06 00:00:00'
#城市备用
#dq=dq[['out_station_time','site_name','overrun_rate','is_truck','city','county']]
#区县备用
    else:
        dq.columns = ['out_station_time', 'site_name', 'overrun_rate', 'is_truck', 'county']
        #dq=dq[['out_station_time','site_name','overrun_rate','is_truck','county']]
        #print(dq.head())
        bins=[-1,0,20,50,100,1000]
        print('--------')
        dq[['overrun_rate']] = dq[['overrun_rate']].astype(float)
        #print(type(dq['overrun_rate'][1]))
        nn=dq.groupby(['site_name']).apply(lambda x:pd.cut(x['overrun_rate'],bins).value_counts(sort = False)).stack().unstack()
        print('++++++')

        nn.columns=['非超限数',"超限<=20%车辆数","20%<超限<=50%车辆数","50%超限<=100%车辆数","超限>100%车辆数"]

        nn["货车流量"]=nn.sum(axis=1)
        nn["超限货车"]=nn[["超限<=20%车辆数","20%<超限<=50%车辆数","50%超限<=100%车辆数","超限>100%车辆数"]].sum(axis=1)
        nn['平均超限率(%)']=nn["超限货车"]/nn["货车流量"]
        nn['超限<=20%占比(%)']=nn["超限<=20%车辆数"]/nn["超限货车"]
        nn['20%<超限<=50%占比(%)']=nn["20%<超限<=50%车辆数"]/nn["超限货车"]
        nn['50%超限<=100%占比(%)']=nn["50%超限<=100%车辆数"]/nn["超限货车"]
        nn['超限>100%占比(%)']=nn["超限>100%车辆数"]/nn["超限货车"]
        nn = nn.fillna(0, inplace=False)

    #数据格式处理区
        nn['平均超限率(%)']=nn['平均超限率(%)'].map(lambda x:format(x,'.2%'))
        nn['超限<=20%占比(%)']=nn['超限<=20%占比(%)'].map(lambda x:format(x,'.2%'))
        nn['20%<超限<=50%占比(%)']=nn['20%<超限<=50%占比(%)'].map(lambda x:format(x,'.2%'))
        nn['50%超限<=100%占比(%)']=nn['50%超限<=100%占比(%)'].map(lambda x:format(x,'.2%'))
        nn['超限>100%占比(%)']=nn['超限>100%占比(%)'].map(lambda x:format(x,'.2%'))
        nn.index.name = '站点'
        nn=nn.reset_index()
        nn = nn.fillna(0, inplace=False)

        mm=nn[['站点','货车流量','超限货车','平均超限率(%)','超限<=20%车辆数','超限<=20%占比(%)','20%<超限<=50%车辆数','20%<超限<=50%占比(%)','50%超限<=100%车辆数','50%超限<=100%占比(%)','超限>100%车辆数','超限>100%占比(%)']]
        mm_hz=pd.DataFrame(columns=mm.columns)
        mm_hz['站点']=['汇总']
        mm_hz['货车流量']=mm['货车流量'].sum()
        mm_hz['超限货车']=mm['超限货车'].sum()
        mm_hz['平均超限率(%)']=mm['超限货车'].sum()/mm['货车流量'].sum()
        mm_hz['超限<=20%车辆数']=mm["超限<=20%车辆数"].sum()
        mm_hz['超限<=20%占比(%)']=mm["超限<=20%车辆数"].sum()/mm["超限货车"].sum()
        mm_hz['20%<超限<=50%车辆数']=mm["20%<超限<=50%车辆数"].sum()
        mm_hz['20%<超限<=50%占比(%)']=mm["20%<超限<=50%车辆数"].sum()/mm["超限货车"].sum()
        mm_hz['50%超限<=100%车辆数']=mm["50%超限<=100%车辆数"].sum()
        mm_hz['50%超限<=100%占比(%)']=mm["50%超限<=100%车辆数"].sum()/mm["超限货车"].sum()
        mm_hz['超限>100%车辆数']=mm["超限>100%车辆数"].sum()
        mm_hz['超限>100%占比(%)']=mm["超限>100%车辆数"].sum()/mm["超限货车"].sum()
        mm_hz = mm_hz.fillna(0, inplace=False)
#数据格式处理区

        mm_hz['平均超限率(%)'] = mm_hz['平均超限率(%)'].apply(lambda x: format(x, '.2%'))
        mm_hz['超限<=20%占比(%)']= mm_hz['超限<=20%占比(%)'].apply(lambda x: format(x, '.2%'))
        mm_hz['20%<超限<=50%占比(%)'] = mm_hz['20%<超限<=50%占比(%)'].map(lambda x: format(x, '.2%'))
        mm_hz['50%超限<=100%占比(%)'] = mm_hz['50%超限<=100%占比(%)'].map(lambda x: format(x, '.2%'))
        mm_hz['超限>100%占比(%)'] = mm_hz['超限>100%占比(%)'].map(lambda x: format(x, '.2%'))

        boss=pd.concat([mm,mm_hz])
        boss= boss.fillna(0, inplace=False)

        ll = i + "成功读取数据"
        log.append(ll)
        print('==========')
        boss.to_excel(sc_dz+'\\'+i+'重点'+qishi_date.split('-')[-2]+qishi_date.split('-')[-1]+'_'+jiezi_date.split('-')[-2]+jiezi_date.split('-')[-1]+'.xlsx',index=True)
        print(boss.tail(5))
print(log)
