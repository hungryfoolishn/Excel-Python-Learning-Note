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



sql = "SELECT city,county,city_code,county_code FROM t_code_area"
t_code_area=get_df_from_db(sql)

sql = "SELECT area_city,area_county,record_code,CASE_NUM,dept_county,car_no from t_case_sign_result WHERE record_type = 31  and insert_type = 1 and data_source =1 " \
      "and case_type =1 and close_case_time >= '2020-09-01 00:00:00' " \
      "and close_case_time < '2022-03-31 00:00:00' " \
      "and   area_city = 330100"
t_case_sign_result=get_df_from_db(sql)
print(t_case_sign_result)

sql = "SELECT car_number,county,vehicle_owner FROM  t_bas_car_information  "
car_information=get_df_from_db(sql)

sql = "SELECT area_city,area_county,record_code,car_no,out_station_time,status FROM t_bas_over_data_collection_31 WHERE out_station_time >='2020-09-01 00:00:00' AND out_station_time <='2022-03-31 00:00:00' AND area_city = 330100 AND law_judgment=1 and status in (4,6,12)"
t_bas_over_data_collection_31=get_df_from_db(sql)
print(t_bas_over_data_collection_31)


car_information.rename(
    columns={'car_number': 'car_no',
             'county': 'dept_county'}, inplace=True)


sign_car = pd.merge(t_case_sign_result,car_information,on=['car_no','dept_county'],how='inner')
t_case_sign_result= t_case_sign_result.drop_duplicates(['CASE_NUM'])
sign_car= sign_car.drop_duplicates(['CASE_NUM'])
总结案_sign_car_num=t_case_sign_result.groupby([t_case_sign_result['area_city'],t_case_sign_result['area_county']])['CASE_NUM'].count().reset_index(name='处罚总数')
车籍地_sign_car_num=sign_car.groupby([sign_car['area_city'],sign_car['area_county']])['CASE_NUM'].count().reset_index(name='车籍地处罚数')
h_case_num= pd.merge(总结案_sign_car_num,车籍地_sign_car_num,on=['area_city','area_county'],how='left')
h_case_num['违法地发生处罚数']=h_case_num['处罚总数']-h_case_num['车籍地处罚数']


cscase_num = t_bas_over_data_collection_31.groupby([t_bas_over_data_collection_31['area_city'], t_bas_over_data_collection_31['area_county']])['record_code'].count().reset_index(name='初始数据')

t_bas_over_data_collection_31 = t_bas_over_data_collection_31[(t_bas_over_data_collection_31.status == 4)
                     | (t_bas_over_data_collection_31.status  == 6)
                     | (t_bas_over_data_collection_31.status  == 12)
                     ]
无地址= pd.merge(t_bas_over_data_collection_31,car_information,on=['car_no'],how='left')
无地址.to_excel("C:/Users/Administrator/Desktop/无地址.xlsx")
总计=无地址['record_code'].count()
print(总计)
有地址数=无地址['vehicle_owner'].count()
无地址数=总计-有地址数

print('无地址数',无地址数)

cxcase_num = t_bas_over_data_collection_31.groupby([t_bas_over_data_collection_31['area_city'], t_bas_over_data_collection_31['area_county']])['record_code'].count().reset_index(name='存续数')
q_case_num= pd.merge(cscase_num,cxcase_num,on=['area_city','area_county'],how='left')
最终= pd.merge(q_case_num,h_case_num,on=['area_city','area_county'],how='outer')
最终= pd.merge(最终,t_code_area,left_on=['area_county'],right_on=['county_code'],how='left')


# h_case_num.to_excel("C:/Users/Administrator/Desktop/h_case_num.xlsx")
# q_case_num.to_excel("C:/Users/Administrator/Desktop/q_case_num.xlsx")
最终.to_excel("C:/Users/Administrator/Desktop/杭州市治超非现场执法车籍地处罚统计表.xlsx")




# U_源头_区域表 = pd.merge(t_bas_pass_data_71 ,t_code_area,left_on='area_county',right_on='county_code',how='left')
#
# 企业_源头表= pd.merge(t_bas_source_company,t_bas_source_company_equipment,left_on='id',right_on='source_company_id',how='left')
#
# 企业_源头_站点表 = pd.merge(企业_源头表,t_sys_station,left_on='station_code',right_on='station_code',how='left')