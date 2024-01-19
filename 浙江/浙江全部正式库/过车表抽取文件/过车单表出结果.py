import pandas as pd
import pymysql
from sqlalchemy import create_engine
import time
import schedule
from datetime import datetime
from datetime import timedelta


pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)

'''接口区'''
# db = pymysql.connect(host='192.168.2.119',user='zjzhzcuser',passwd='F4dus0ee',
#             port=3306,charset='utf8',database='db_manage_overruns')
engine_zs=create_engine('mysql+pymysql://zjzhzcuser:F4dus0ee@192.168.2.119:3306/db_manage_overruns')

# db = pymysql.connect(host="192.168.1.229", port=3306, user='root', password='zcits123456',
#                      database='db_manage_overruns')
# engine_cs=create_engine('mysql+pymysql://root:zcits123456@192.168.1.229:3306/db_manage_overruns')
#
# ''''''
# engine=create_engine('mysql+pymysql://root:jingdong@localhost:3308/jingdong_ceshi')

def get_data_from_sql(sql, cs_list):
    global db
    db = pymysql.connect(host='192.168.2.119',user='zjzhzcuser',passwd='F4dus0ee',
            port=3306,charset='utf8',database='db_manage_overruns')
    # db = pymysql.connect(host="192.168.1.229",port=3306,
    #                      user='root', password='zcits123456',
    #                      database='db_manage_overruns')

    cursor = db.cursor()
    cursor.execute(sql, cs_list)
    data = cursor.fetchall()
    '''空列表与非空列表均可以'''
    data=pd.DataFrame(columns=[i[0] for i in cursor.description],data=list(data))
    return data

def get_data_from_sql_w(sql):
    global db
    db = pymysql.connect(host='192.168.2.119',user='zjzhzcuser',passwd='F4dus0ee',
            port=3306,charset='utf8',database='db_manage_overruns')
    # db = pymysql.connect(host="192.168.1.229",port=3306,
    #                      user='root', password='zcits123456',
    #                      database='db_manage_overruns')

    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    '''空列表与非空列表均可以'''
    data=pd.DataFrame(columns=[i[0] for i in cursor.description],data=list(data))
    return data

def foo():
    now_time = datetime.now()
    jj=now_time.strftime('%Y-%m-%d-%H-%M')
    tt = now_time.strftime('%Y-%m-%d')
    nn = tt+' 00:00:00'
    #tt='2020-01-01'
    # nn='2020-01-01 00:00:00'

    tod = datetime.strptime(tt, '%Y-%m-%d')
    print(tod)
    sql_606="select area_province,area_city,area_county,out_station,site_name,count(*) as 'car_no'\
            from t_bas_pass_data_31 \
            where date(out_station_time)=%s and area_county=330781 \
            group by area_province,area_city,area_county,out_station,site_name"
    sql_808= "select area_province,area_city,area_county,out_station,site_name,\
            date(out_station_time) as 'date_bj',out_station_time,car_no,total_weight,\
            limit_weight,overrun,overrun_rate,car_no_color,axis,\
            allow,speed,status,is_truck \
            from t_bas_pass_data_31 \
            where date(out_station_time)=%s and is_truck=1 and area_county=330781"
    sql_2 = "select province,province_code,city,city_code,county,county_code from t_code_area where county='兰溪'"

    print('''1''')
    '''获取货车'''
    df = get_data_from_sql(sql_808, [tt])
    df.fillna(0, inplace=True)

    print('''2''')
    '''获取过车'''
    t0=get_data_from_sql(sql_606, [tt])
    d0=df.groupby(['area_province', 'area_city', 'area_county', 'out_station','site_name'])['is_truck'].count().reset_index(name='is_truck')
    print('ggggg')
    print(d0.head())
    print('''3''')

    '''获取浙江省地市编码'''
    df2 = get_data_from_sql_w(sql_2)

    print(tt)
    print(nn)



    if df.empty==False:
        print(3)
        dd = pd.DataFrame({'city': ['杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州', '杭州',
                                    '杭州', '杭州', '杭州', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波', '宁波',
                                    '宁波', '宁波', '温州', '温州', '温州', '温州', '温州', '温州', '温州', '温州', '温州', '温州', '温州', '温州',
                                    '温州', '嘉兴', '嘉兴', '嘉兴', '嘉兴', '嘉兴', '嘉兴', '嘉兴', '嘉兴', '湖州', '湖州', '湖州', '湖州', '湖州',
                                    '湖州', '湖州', '绍兴', '绍兴', '绍兴', '绍兴', '绍兴', '绍兴', '绍兴', '绍兴', '金华', '金华', '金华', '金华',
                                    '金华', '金华', '金华', '金华', '金华', '金华', '衢州', '衢州', '衢州', '衢州', '衢州', '衢州', '衢州', '舟山',
                                    '舟山', '舟山', '舟山', '舟山', '台州', '台州', '台州', '台州', '台州', '台州', '台州', '台州', '台州', '台州',
                                    '丽水', '丽水', '丽水', '丽水', '丽水', '丽水', '丽水', '丽水', '丽水', '丽水']
                              ,
                           'city_code':['330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330100', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330200', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330300', '330400', '330400', '330400', '330400', '330400', '330400', '330400', '330400', '330500', '330500', '330500', '330500', '330500', '330500', '330500', '330600', '330600', '330600', '330600', '330600', '330600', '330600', '330600', '330700', '330700', '330700', '330700', '330700', '330700', '330700', '330700', '330700', '330700', '330800', '330800', '330800', '330800', '330800', '330800', '330800', '330900', '330900', '330900', '330900', '330900', '331000', '331000', '331000', '331000', '331000', '331000', '331000', '331000', '331000', '331000', '331100', '331100', '331100', '331100', '331100', '331100', '331100', '331100', '331100', '331100']


                              ,
                           'county': ['市辖区', '上城', '下城', '江干', '拱墅', '西湖', '下沙', '滨江', '萧山', '余杭', '临平', '桐庐', '淳安', '钱塘新区',
                                      '建德', '富阳', '临安', '市辖区', '海曙', '江东', '江北', '北仑', '镇海', '鄞州', '象山', '宁海', '东钱湖', '余姚',
                                      '慈溪', '奉化', '市辖区', '鹿城', '龙湾', '瓯海', '洞头', '永嘉', '平阳', '苍南', '文成', '泰顺', '瑞安', '乐清',
                                      '龙港', '市辖区', '南湖', '秀洲', '嘉善', '海盐', '海宁', '平湖', '桐乡', '市辖区', '吴兴', '南浔', '德清', '长兴',
                                      '安吉', '南太湖新区', '市辖区', '越城', '柯桥', '上虞', '绍兴县', '新昌', '诸暨', '嵊州', '市辖区', '婺城', '金东',
                                      '武义', '浦江', '磐安', '兰溪', '义乌', '东阳', '永康', '市辖区', '柯城', '衢江', '常山', '开化', '龙游', '江山',
                                      '市辖区', '定海', '普陀', '岱山', '嵊泗', '市辖区', '椒江', '黄岩', '路桥', '玉环', '三门', '天台', '仙居', '温岭',
                                      '临海', '市辖区', '莲都', '青田', '缙云', '遂昌', '松阳', '云和', '庆元', '景宁', '龙泉']
                              ,
                           'county_code':['330101', '330102', '330103', '330104', '330105', '330106', '330107', '330108', '330109', '330110', '330113', '330122', '330127', '330155', '330182', '330183', '330185', '330201', '330203', '330204', '330205', '330206', '330211', '330212', '330225', '330226', '330256', '330281', '330282', '330283', '330301', '330302', '330303', '330304', '330322', '330324', '330326', '330327', '330328', '330329', '330381', '330382', '330383', '330401', '330402', '330411', '330421', '330424', '330481', '330482', '330483', '330501', '330502', '330503', '330521', '330522', '330523', '330552', '330601', '330602', '330603', '330604', '330621', '330624', '330681', '330683', '330701', '330702', '330703', '330723', '330726', '330727', '330781', '330782', '330783', '330784', '330801', '330802', '330803', '330822', '330824', '330825', '330881', '330901', '330902', '330903', '330921', '330922', '331001', '331002', '331003', '331004', '331021', '331022', '331023', '331024', '331081', '331082', '331101', '331102', '331121', '331122', '331123', '331124', '331125', '331126', '331127', '331181']


                              ,
                           '系数': [1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.2, 1.1, 1.1, 1.1, 1.21, 1.1, 1.0,
                                  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                                  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0,
                                  1.0, 1.0, 1.03, 1.0, 1.0, 1.0, 1.1, 1.2, 1.0, 1.2, 1.1, 1.0, 1.0, 1.0, 1.2, 1.0, 1.0, 1.0,
                                  1.0, 1.1, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1,
                                  1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1]
                           })
        print(dd.city_code.astype('str').tolist())
        print(dd.county_code.astype('str').tolist())

        t_1 = pd.merge(df, dd, left_on='area_county', right_on='county_code', how='left')

        t_1['bj'] = 1
        t_1['bg'] = 1
        t_1['date_bj'] = nn

        '''车流量相关数量t0'''
        '''车流量，货车辆'''
        '''车牌标记'''
        t_1['car_no_bj'] = t_1['car_no'].astype('str').apply(lambda x: 0 if len(x) == 7 else 1)
        print(t_1.head())
        print('京东')
        print(t_1['car_no_bj'].value_counts())




        '''货车的相关数量t1'''
        t1=t_1[t_1['is_truck']==1]
        if t1.empty==False:
        #t1['total_weight'] = t1['total_weight'].astype('float')

            print(t1.columns)
            print(t1.shape[1])
            print(t1.shape[0])
            print(t1.dropna(subset=['total_weight']).shape[0])
            '''超限率构造'''
            t1['total_weight']=t1['total_weight'].astype('float')
            t1['limit_weight']=t1['limit_weight'].astype('float')
            t1['系数'] = t1['系数'].astype('float')

            t1['new_overrun_rate_jisuan'] = t1.apply(lambda x: '{:.2f}'.format(\
                (x['total_weight'] - x['limit_weight'] * x['系数']) / (x['limit_weight'] * x['系数'] + 0.0001) * 100), axis=1)
            print(1)

            '''超限率与重量校正'''
            t1['new_overrun_rate'] = t1['new_overrun_rate_jisuan'].astype('float').apply(lambda x: 0 if x < 0 else x).apply(
                lambda x: x if (x >= 0 and x < 800) else 999)
            t1['total_weight'] = t1['total_weight'].apply(lambda x: x if (x > 0 and x < 300) else 999)

            print(2)
        else:
            t1 = t_1[t_1['is_truck'] == 1]


        '''参数设置'''
        # 'date_bj', 'area_province', 'area_city', 'area_county', 'site_name'
        tl = pd.DataFrame({'area_province': [333333] * 14, 'area_city': [111111] * 14, 'area_county': [111111] * 14,
                           'site_name': [111111] * 14,'out_station': [3333333333] * 14,'car_no':[1111111]*14,
                           'new_overrun_rate': [0,5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 800, 999],
                           'total_weight': [0,5,10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 300, 999]})
        t1 = pd.concat([t1, tl], axis=0)



        print('--5--')
        print(t1.tail())
        '''超限率分桶'''
        bin_l = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 800, 1000]
        labels_l = ['tl_0_10_bj', 'tl_10_20_bj', 'tl_20_30_bj', 'tl_30_40_bj',
                    'tl_40_50_bj', 'tl_50_60_bj', 'tl_60_70_bj', 'tl_70_80_bj',
                    'tl_80_90_bj', 'tl_90_100_bj', 'tl_100_bj', 'tl_yc']
        bin_t = [0,5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 300, 1000]
        labels_t = ['tt_0_5_bj','tt_5_10_bj','tt_10_20_bj',
                    'tt_20_30_bj', 'tt_30_40_bj', 'tt_40_50_bj', 'tt_50_60_bj',
                    'tt_60_70_bj', 'tt_70_80_bj', 'tt_80_90_bj', 'tt_90_100_bj',
                    'tt_100_bj', 'tt_yc']

        t1['new_overrun_rate_b'] = pd.cut(x=t1['new_overrun_rate'], bins=bin_l, labels=labels_l, include_lowest=True)
        t1['total_weight_b'] = pd.cut(x=t1['total_weight'], bins=bin_t, labels=labels_t, include_lowest=True)


        t1['new_overrun_rate_b'] = t1['new_overrun_rate_b'].astype('str')
        t1['total_weight_b'] = t1['total_weight_b'].astype('str')

        print('---1---')
        print(t1[['new_overrun_rate_b']].tail())
        print(t1[['total_weight_b']].tail())
        print('---2---')




        print(t1.shape[0])
        start_3 = time.time()
        '''无车牌数与超限数'''
        t1['car_no_bj'] = t1['car_no'].astype('str').apply(lambda x: 0 if len(x) == 7 else 1)
        t1['over_bj'] = t1['new_overrun_rate'].apply(lambda x: 0 if x == 0 else 1)
        tw = t1.groupby(['area_province', 'area_city', 'area_county', 'out_station', 'site_name']).agg(
            {'car_no_bj': 'sum','over_bj': 'sum'}).reset_index()

        print(3)
        t1_1 = t1.pivot_table(index=[ 'area_province', 'area_city', 'area_county','out_station', 'site_name'],
                              columns=['new_overrun_rate_b'],
                              values=['bj'],
                              aggfunc='sum',
                              margins=False).reset_index()
        t1_1.columns = t1_1.columns.droplevel(0)
        #t1_1.to_excel(r"F:\报表输出记录\8月\2022-08-15\单日过车货车聚合—3l-异常问题.xlsx", index=False)
        t1_1.columns = [ 'area_province', 'area_city', 'area_county', 'out_station','site_name', 'tl_0_10_bj', 'tl_100_bj',
                        'tl_10_20_bj',
                        'tl_20_30_bj', 'tl_30_40_bj', 'tl_40_50_bj', 'tl_50_60_bj',
                        'tl_60_70_bj', 'tl_70_80_bj', 'tl_80_90_bj', 'tl_90_100_bj', 'tl_yc']
        #t1_1.to_excel(r"F:\报表输出记录\8月\2022-08-15\单日过车货车聚合—3l.xlsx", index=False)

        t2_1 = t1.pivot_table(index=[ 'area_province', 'area_city', 'area_county','out_station', 'site_name'],
                              columns=['total_weight_b'],
                              values=['bj'],
                              aggfunc='sum',
                              margins=False).reset_index()

        t2_1.columns = t2_1.columns.droplevel(0)
        #t2_1.to_excel(r"F:\报表输出记录\8月\2022-08-15\单日过车货车聚合—3t-异常问题.xlsx", index=False)
        '''异常'''
        print('滴滴滴')

        t2_1.columns = [ 'area_province', 'area_city', 'area_county', 'out_station','site_name', 'tt_0_5_bj', 'tt_100_bj',
                        'tt_10_20_bj',
                        'tt_20_30_bj', 'tt_30_40_bj', 'tt_40_50_bj', 'tt_50_60_bj','tt_5_10_bj',
                        'tt_60_70_bj', 'tt_70_80_bj', 'tt_80_90_bj', 'tt_90_100_bj', 'tl_yc']
        #t2_1.to_excel(r"F:\报表输出记录\8月\2022-08-15\单日过车货车聚合—3t.xlsx", index=False)

        boss=pd.concat([t0.set_index(['area_province', 'area_city', 'area_county', 'out_station','site_name']),
                        d0.set_index(['area_province', 'area_city', 'area_county', 'out_station', 'site_name']),
                        tw.set_index(['area_province', 'area_city', 'area_county', 'out_station', 'site_name']),
                        t1_1.set_index(['area_province', 'area_city', 'area_county', 'out_station','site_name']), \
                        t2_1.set_index(['area_province', 'area_city', 'area_county','out_station', 'site_name'])],
                        axis=1).reset_index().fillna(0)

        #boss.to_excel(r"F:\报表输出记录\8月\2022-08-16\单日过车货车聚合—558.xlsx", index=True)

        BB=pd.merge(boss,df2,left_on=['area_county'],right_on=['county_code'],how='left')
        #BB.to_excel(r"F:\报表输出记录\8月\2022-08-15\单日过车货车聚合—788.xlsx", index=False)
        print(BB.columns)
        '''['area_province', 'area_city', 'area_county', 'out_station', 'site_name',
           'car_no', 'is_truck', 'car_no_bj', 'over_bj', 'tl_0_10_bj', 'tl_100_bj',
           'tl_10_20_bj', 'tl_20_30_bj', 'tl_30_40_bj', 'tl_40_50_bj',
           'tl_50_60_bj', 'tl_60_70_bj', 'tl_70_80_bj', 'tl_80_90_bj',
           'tl_90_100_bj', 'tl_yc', 'tt_0_10_bj', 'tt_100_bj', 'tt_10_20_bj',
           'tt_20_30_bj', 'tt_30_40_bj', 'tt_40_50_bj', 'tt_50_60_bj',
           'tt_60_70_bj', 'tt_70_80_bj', 'tt_80_90_bj', 'tt_90_100_bj', 'tl_yc',
           'province', 'province_code', 'city', 'city_code', 'county',
           'county_code']'''
        BB.columns=['area_province', 'city_code', 'county_code', 'station_code', 'station_name',
           'pass_num', 'truck_num', 'no_car_num', 'overrun_num', 'overrun_0_10', 'overrun_100',
           'overrun_10_20', 'overrun_20_30', 'overrun_30_40', 'overrun_40_50',
           'overrun_50_60', 'overrun_60_70', 'overrun_70_80', 'overrun_80_90',
           'overrun_90_100', 'overrun_yc', 'total_weight_0_5', 'total_weight_100', 'total_weight_10_20',
           'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50', 'total_weight_50_60','total_weight_5_10',
           'total_weight_60_70', 'total_weight_70_80', 'total_weight_80_90', 'total_weight_90_100', 'total_weight_yc',
           'province', 'province_code', 'city_name', 'city_code_1', 'county_name',
           'county_code_1']
        BB['station_status']=1
        BB['station_type']=31
        BB['statistic_date']=tt
        BB=BB[['statistic_date','city_code','city_name', 'county_code', 'county_name', 'station_code', 'station_name','station_status','station_type',
           'pass_num', 'truck_num','overrun_num', 'no_car_num',  'overrun_0_10',
           'overrun_10_20', 'overrun_20_30', 'overrun_30_40', 'overrun_40_50',
           'overrun_50_60', 'overrun_60_70', 'overrun_70_80', 'overrun_80_90',
           'overrun_90_100', 'overrun_100', 'overrun_yc', 'total_weight_0_5', 'total_weight_5_10','total_weight_10_20',
           'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50', 'total_weight_50_60',
           'total_weight_60_70', 'total_weight_70_80', 'total_weight_80_90', 'total_weight_90_100','total_weight_100', 'total_weight_yc'
           ]]
        BB[['pass_num', 'truck_num','overrun_num', 'no_car_num',  'overrun_0_10',
           'overrun_10_20', 'overrun_20_30', 'overrun_30_40', 'overrun_40_50',
           'overrun_50_60', 'overrun_60_70', 'overrun_70_80', 'overrun_80_90',
           'overrun_90_100', 'overrun_100', 'overrun_yc', 'total_weight_0_5', 'total_weight_5_10','total_weight_10_20',
           'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50', 'total_weight_50_60',
           'total_weight_60_70', 'total_weight_70_80', 'total_weight_80_90', 'total_weight_90_100','total_weight_100', 'total_weight_yc']]=BB[['pass_num', 'truck_num','overrun_num', 'no_car_num',  'overrun_0_10',
           'overrun_10_20', 'overrun_20_30', 'overrun_30_40', 'overrun_40_50',
           'overrun_50_60', 'overrun_60_70', 'overrun_70_80', 'overrun_80_90',
           'overrun_90_100', 'overrun_100', 'overrun_yc', 'total_weight_0_5', 'total_weight_5_10','total_weight_10_20',
           'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50', 'total_weight_50_60',
           'total_weight_60_70', 'total_weight_70_80', 'total_weight_80_90', 'total_weight_90_100','total_weight_100', 'total_weight_yc']].astype('int')
        start_2 = time.time()

        '''数据库删除'''
        # db = pymysql.connect(host="192.168.1.229", port=3306, user='root', password='zcits123456',
        #                  database='db_manage_overruns')
        # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
        # #                  database='jingdong_ceshi')
        # mycursor = db.cursor()
        # sql = "DELETE FROM t_bas_basic_data_pass WHERE statistic_date = '{}'".format(tt)
        # mycursor.execute(sql)
        # db.commit()
        # db.close()
        '''存数据库'''
        # print(start_2 - start_1)
        # start_3=time.time()
        # BB[BB['city_code']!=111111].to_sql('t_bas_basic_data_pass', engine_zs, if_exists='append', index=False)
        '''存excel表'''
        print('运行将要结束')
        BB[BB['city_code'] != 111111].to_excel(r"C:\Users\Administrator\Desktop\主表定时测试"+'\\'+jj+'.xlsx',index=False)
        start_4 = time.time()
        print(start_4 - start_3)
    else:
        pass


#schedule.every().days.at("13:51").do(foo)
#schedule.every(1).minutes.do(foo)
schedule.every(10).seconds.do(foo)
while True:
    # 保持schedule一直运行，然后去查询上面的任务
    schedule.run_pending()
