import pymysql
import pandas as pd
import  time
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.styles import Border,Side
from openpyxl.styles import Alignment

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

'''案件处理累计表'''
""" 引入原始表 """
#q='2022-01-01'
#qq = input("请输入起始年月日(比如2022-01-01)：")
ss = input("请输入截止年月日(比如2022-05-01)：")
#cs = input("请输入省市名：")

# q_list=['2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01','2022-12-01']
# s_list=['2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01','2022-12-01','2023-01-01',]
q_list=['2022-01-01','2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01']
s_list=[ss,'2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01']
print(s_list)
wj_title=['汇总','1月明细','2月明细','3月明细','4月明细','5月明细','6月明细','7月明细']

# start=time.time()
sql = "SELECT * FROM t_code_area"
t_code_area=get_df_from_db(sql)
print('开始')
for q,s,js in zip(q_list,s_list,wj_title):
    print(q,s,js)
    sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(q,s)
    t_bas_over_data_collection_31=get_df_from_db(sql)
    print('开始-1')
    sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(q,s)
    t_case_sign_result=get_df_from_db(sql)
    print('开始-2')

    q=q.replace('-','')
    s=s.replace('-','')
    sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >={} and insert_time <{}".format(q,s)
    t_bas_over_data_collection_makecopy=get_df_from_db(sql)

    sql = "SELECT * FROM t_sys_station "
    t_sys_station=get_df_from_db(sql)

    # end =time.time()
    # print(end-start)

    U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31,t_code_area,left_on='area_county',right_on='county_code',how='left')

    U_案件处罚_区域表= pd.merge(t_case_sign_result,t_code_area,left_on='area_county',right_on='county_code',how='left')

    U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy,t_code_area,left_on='area_county',right_on='county_code',how='left')

    T_入库查询=U_检测点_区域表.loc[(U_检测点_区域表['law_judgment']=="1")]


    '''滴滴滴'''
    T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                            & (U_案件处罚_区域表.insert_type == 5)]

    T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                         & (U_案件处罚_区域表.insert_type == 1)
                         & (U_案件处罚_区域表.data_source == 1)
                         & (U_案件处罚_区域表.case_type == 1)]

    T_外省抄告 = U_外省抄告_区域表


    T_入库查询 = T_入库查询.sort_values(['area_city','area_county'], ascending=False)
    print('---1---')
    print(T_入库查询.columns)
    T_入库数 = T_入库查询.groupby(['area_city','city','area_county','county']).count()

    T_入库数 = T_入库数.loc[:, ['id_x']]
    T_入库数.rename(columns={'id_x': '入库数(系统)'}, inplace=True)

    T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
    T_现场处罚计数 = T_现场处罚.groupby(['city','county'])['CASE_NUM'].count()
    T_现场处罚计数 = T_现场处罚计数.to_frame()
    T_现场处罚计数.rename(columns={'CASE_NUM': '现场处罚(系统)'}, inplace=True)

    T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数 = T_非现场处罚.groupby(['city','county'])['CASE_NUM'].count()
    T_非现场处罚计数 = T_非现场处罚计数.to_frame()
    T_非现场处罚计数.rename(columns={'CASE_NUM': '非现场处罚(系统)'}, inplace=True)

    T_外省抄告 = T_外省抄告.groupby(['city','county']).count()
    T_外省抄告计数 = T_外省抄告.loc[:, ['id_x']]
    T_外省抄告计数.rename(columns={'id_x': '外省抄告(系统)'}, inplace=True)
    W_案件统计 = pd.merge(T_入库数, T_现场处罚计数, left_on=['city','county'], right_on=['city','county'], how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, left_on=['city','county'], right_on=['city','county'], how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, left_on=['city','county'], right_on=['city','county'], how='outer')
    W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
    W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0

    '''其他市案件统计情况'''
    W_案件统计 =W_案件统计.reset_index()
    #W_案件统计= W_案件统计.drop(index=(W_案件统计.loc[(W_案件统计['county'] == '市辖区')].index))
    W_案件统计.to_excel(r"C:\Users\Administrator\Desktop\lll\案件"+js+"表.xlsx",index=False)
    print(js)

    #print(W_案件统计.head(30))