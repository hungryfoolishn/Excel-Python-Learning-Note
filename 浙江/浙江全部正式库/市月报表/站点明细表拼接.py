import pymysql
import pandas as pd
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


df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=False)



df_报修点位统计表= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")
df_报修点位统计表.columns = df_报修点位统计表.iloc[0]
df_报修点位统计表= df_报修点位统计表.iloc[1:].reset_index(drop=False)






sql = "SELECT station_name FROM t_sys_station where is_match_station =1 and station_type = 31"
t_station_name= get_df_from_db(sql)

print(t_station_name)

station_name=t_station_name['station_name']

df_数据汇总.loc[df_数据汇总['站点名称'].isin(station_name), '最后接收时间'] = '是'
df_数据汇总.loc[~(df_数据汇总['站点名称'].isin(station_name)), '最后接收时间'] = '否'
df_报修点位统计表.loc[df_报修点位统计表['报修站点名称'].isin(station_name), '备注'] = '是'
df_报修点位统计表.loc[~(df_报修点位统计表['报修站点名称'].isin(station_name)), '备注'] = '否'



def DF2Excel(data_path, data_list, sheet_name_list):
    '''将多个dataframe 保存到同一个excel 的不同sheet 上
    参数：
    data_path：str
        需要保存的文件地址及文件名
    data_list：list
        需要保存到excel的dataframe
    sheet_name_list：list
        sheet name 每个sheet 的名称
    '''

    write = pd.ExcelWriter(data_path)
    for da, sh_name in zip(data_list, sheet_name_list):
        da.to_excel(write, sheet_name=sh_name, index=False)

    # 必须运行write.save()，不然不能输出到本地
    write.save()



I = ["杭州","宁波","温州","嘉兴","湖州","绍兴","金华","衢州","舟山","台州","丽水"]
data_list = []
sheet_name_list = []
for i in I:
    test1 = df_数据汇总.loc[(df_数据汇总.地市 == "{}".format(i))]
    test2 = df_报修点位统计表.loc[(df_报修点位统计表.地市 == "{}".format(i))]
    test2 = pd.DataFrame(test2, columns=["报修站点名称", "地市", "区县","备注"])
    test1 = pd.DataFrame(test1, columns=["站点名称", "地市", "区县", "理应在线天数", "实际在线天数", "在线率", "货车数", "超限数", "超限10%除外数",
                                         "超限10%除外超限率(%)", "超限20%除外数", "超限20%除外超限率(%)", "百吨王数", "超限100%数", "超限率(%)","最后接收时间"])

    name = pd.concat((test1, test2), axis=1, ignore_index=False)
    data_list.append(name)
    sheet_name_list.append(i)


# ---------- 调用函数 ------------------

# 需要保存的文件地址及文件名
data_path = r'C:/Users/Administrator/Desktop/系统拉出表/省级月报/站点超限明细.xlsx'
# 调用函数
DF2Excel(data_path, data_list, sheet_name_list)
