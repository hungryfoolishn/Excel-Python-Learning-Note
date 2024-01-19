import pymysql
import pandas as pd
import numpy as np
import  time


db = pymysql.connect(
    host='192.168.2.119',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=3306,
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
q = input("请输入起始年月日(比如2022-01-01)：")
s = input("请输入截止年月日(比如2022-05-01)：")
cs = input("请输入省市名：")


start=time.time()
sql = "SELECT * FROM t_code_area"
t_code_area=get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(q,s)
t_bas_over_data_collection_31=get_df_from_db(sql)
sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(q,s)
t_case_sign_result=get_df_from_db(sql)

q=q.replace('-','')
s=s.replace('-','')
sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >={} and insert_time <{}".format(q,s)
t_bas_over_data_collection_makecopy=get_df_from_db(sql)

sql = "SELECT * FROM t_sys_station "
t_sys_station=get_df_from_db(sql)

end =time.time()
print(end-start)

U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31,t_code_area,left_on='area_county',right_on='county_code',how='left')

U_案件处罚_区域表= pd.merge(t_case_sign_result,t_code_area,left_on='area_county',right_on='county_code',how='left')

U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy,t_code_area,left_on='area_county',right_on='county_code',how='left')

T_入库查询=U_检测点_区域表.loc[(U_检测点_区域表['law_judgment']=="1")  ]

if cs == '浙江':
    T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                        & (U_案件处罚_区域表.insert_type == 5)
                        & (U_案件处罚_区域表.area_province == '330000')
                        ]

    T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                         & (U_案件处罚_区域表.insert_type == 1)
                         & (U_案件处罚_区域表.data_source == 1)
                         & (U_案件处罚_区域表.case_type == 1)
                         & (U_案件处罚_区域表.area_province == '330000')
                         ]

    T_外省抄告 = U_外省抄告_区域表

    T_入库查询 = T_入库查询.sort_values(['area_city'], ascending=False).reset_index(drop=True)
    T_入库数 = T_入库查询.groupby([T_入库查询['area_city'], T_入库查询['city']]).count()

    T_入库数 = T_入库数.loc[:, ['id_x']]
    T_入库数.rename(columns={'id_x': '入库数(系统)'}, inplace=True)

    T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
    T_现场处罚计数 = T_现场处罚.groupby([T_现场处罚['city']])['CASE_NUM'].count()
    T_现场处罚计数 = T_现场处罚计数.to_frame()
    T_现场处罚计数.rename(columns={'CASE_NUM': '现场处罚(系统)'}, inplace=True)

    T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数 = T_非现场处罚.groupby([T_非现场处罚['city']])['CASE_NUM'].count()
    T_非现场处罚计数 = T_非现场处罚计数.to_frame()
    T_非现场处罚计数.rename(columns={'CASE_NUM': '非现场处罚(系统)'}, inplace=True)

    T_外省抄告 = T_外省抄告.groupby([T_外省抄告['city']]).count()
    T_外省抄告计数 = T_外省抄告.loc[:, ['id_x']]
    T_外省抄告计数.rename(columns={'id_x': '外省抄告(系统)'}, inplace=True)

    W_案件统计 = pd.merge(T_入库数, T_现场处罚计数, left_on='city', right_on='city', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, left_on='city', right_on='city', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, left_on='city', right_on='city', how='outer')
    W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
    W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
    print(W_案件统计)
    W_案件统计.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/2022年浙江处罚案件情况统计.xlsx")


else:
    I = ["杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"]
    for cs in I:
        T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                            & (U_案件处罚_区域表.insert_type == 5)
                            & (U_案件处罚_区域表.city == '{}'.format(cs))
                            ]

        T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                             & (U_案件处罚_区域表.insert_type == 1)
                             & (U_案件处罚_区域表.data_source == 1)
                             & (U_案件处罚_区域表.case_type == 1)
                             & (U_案件处罚_区域表.city == '{}'.format(cs))
                             ]

        T_外省抄告 = U_外省抄告_区域表
        T_外省抄告 = T_外省抄告[(T_外省抄告.city == '{}'.format(cs))
        ]
        T_入库查询 = T_入库查询[(T_入库查询.city == '{}'.format(cs))
        ]

        T_入库查询 = T_入库查询.sort_values(['area_county'], ascending=False).reset_index(drop=True)
        T_入库数 = T_入库查询.groupby([T_入库查询['area_county'], T_入库查询['county']]).count()

        T_入库数 = T_入库数.loc[:, ['id_x']]
        T_入库数.rename(columns={'id_x': '入库数(系统)'}, inplace=True)

        T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
        T_现场处罚计数 = T_现场处罚.groupby([T_现场处罚['county']])['CASE_NUM'].count()
        T_现场处罚计数 = T_现场处罚计数.to_frame()
        T_现场处罚计数.rename(columns={'CASE_NUM': '现场处罚(系统)'}, inplace=True)

        T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
        T_非现场处罚计数 = T_非现场处罚.groupby([T_非现场处罚['county']])['CASE_NUM'].count()
        T_非现场处罚计数 = T_非现场处罚计数.to_frame()
        T_非现场处罚计数.rename(columns={'CASE_NUM': '非现场处罚(系统)'}, inplace=True)

        T_外省抄告 = T_外省抄告.groupby([T_外省抄告['county']]).count()
        T_外省抄告计数 = T_外省抄告.loc[:, ['id_x']]
        T_外省抄告计数.rename(columns={'id_x': '外省抄告(系统)'}, inplace=True)
        W_案件统计 = pd.merge(T_入库数, T_现场处罚计数, left_on='county', right_on='county', how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, left_on='county', right_on='county', how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, left_on='county', right_on='county', how='outer')
        W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
        W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
        if cs == '杭州':
            '''杭州市附件9'''
            附件9 = pd.DataFrame(W_案件统计, columns=["现场处罚(系统)"])
            附件9['现场交警'] = 0
            附件9 = 附件9.fillna(0, inplace=False)
            附件9['非现场查处'] = W_案件统计['非现场处罚(系统)']
            附件9['非现场交警'] = 0
            附件9 = 附件9.fillna(0, inplace=False)
            附件9.rename(columns={'现场处罚(系统)': '现场交通'}, inplace=True)

            print(附件9)

            '''杭州市附件10'''
            附件10 = pd.DataFrame(W_案件统计, columns=["入库数(系统)", "非现场处罚(系统)", "外省抄告(系统)"])
            附件10['处罚率(不含抄告)'] = 附件10['非现场处罚(系统)'] / 附件10['入库数(系统)']
            附件10 = 附件10.fillna(0, inplace=False)
            附件10.loc[附件10['处罚率(不含抄告)'] > 1, '处罚率(不含抄告)'] = 1
            附件10['处罚率(不含抄告)'] = 附件10['处罚率(不含抄告)'].apply(lambda x: format(x, '.2%'))
            附件10['处罚率(含抄告)'] = (附件10['非现场处罚(系统)'] + 附件10['外省抄告(系统)']) / 附件10['入库数(系统)']
            附件10 = 附件10.fillna(0, inplace=False)
            附件10.loc[附件10['处罚率(含抄告)'] > 1, '处罚率(含抄告)'] = 1
            附件10['处罚率(含抄告)'] = 附件10['处罚率(含抄告)'].apply(lambda x: format(x, '.2%'))
            附件10.rename(columns={'入库数(系统)': '入库数(少非现场交警上报)', '非现场处罚(系统)': '处罚数', '外省抄告(系统)': '外省抄告数'}, inplace=True)
            附件10 = 附件10.reset_index()
            附件10 = 附件10.drop(index=(附件10.loc[(附件10['county'] == '市辖区')].index))
            print(附件10)
            with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/{}案件处罚.xlsx'.format(cs))as writer1:
                附件9.to_excel(writer1, sheet_name='附件9', index=True)
                附件10.to_excel(writer1, sheet_name='附件10', index=True)


        elif cs == '湖州':
            '''湖州市附件4'''
            附件10 = pd.DataFrame(W_案件统计, columns=["入库数(系统)", "非现场处罚(系统)", "外省抄告(系统)"])
            附件10['处罚率(不含抄告)'] = 附件10['非现场处罚(系统)'] / 附件10['入库数(系统)']
            附件10 = 附件10.fillna(0, inplace=False)
            附件10['处罚率(不含抄告)'] = 附件10['处罚率(不含抄告)'].apply(lambda x: format(x, '.2%'))
            附件10['处罚率(含抄告)'] = (附件10['非现场处罚(系统)'] + 附件10['外省抄告(系统)']) / 附件10['入库数(系统)']
            附件10 = 附件10.fillna(0, inplace=False)
            附件10.loc[附件10['处罚率(含抄告)'] > 1, '处罚率(含抄告)'] = 1
            附件10['处罚率(含抄告)'] = 附件10['处罚率(含抄告)'].apply(lambda x: format(x, '.2%'))
            附件10.rename(columns={'入库数(系统)': '非现场一次违法判定数（条）', '非现场处罚(系统)': '治超非现场执法查处数（件）', '外省抄告(系统)': '外省抄告数（件）',
                                 '处罚率(含抄告)': '治超非现场执法查处率（%）'}, inplace=True)
            湖州市附件4 = 附件10
            湖州市附件4 = pd.DataFrame(湖州市附件4, columns=["非现场一次违法判定数（条）", "治超非现场执法查处数（件）", "外省抄告数（件）", "治超非现场执法查处率（%）"])
            湖州市附件4 = 湖州市附件4.reset_index()
            湖州市附件4 = 湖州市附件4.drop(index=(湖州市附件4.loc[(湖州市附件4['county'] == '市辖区')].index))
            print(湖州市附件4)
            with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/{}案件处罚.xlsx'.format(cs))as writer1:
                湖州市附件4.to_excel(writer1, sheet_name='附件4', index=True)


        else:
            '''其他市案件统计情况'''
            W_案件统计 = W_案件统计.reset_index()

            print(W_案件统计)
            with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/2022年处罚案件统计情况/{}案件处罚.xlsx'.format(cs))as writer1:
                W_案件统计.to_excel(writer1, sheet_name='案件处罚情况', index=True)


