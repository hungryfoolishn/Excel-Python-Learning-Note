import pandas as pd
import numpy as np



cs = input("请输入省市名：")

df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)
df_接入数= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/接入数.xlsx")
df_报修点位统计= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")
df_报修点位统计.columns = df_报修点位统计.iloc[0]


if cs == '浙江':
    """在线天数大于等于20天货车数大于500的站点数据"""

    T_20天与500筛选 = df_数据汇总[((df_数据汇总.实际在线天数 >= 20)
                           & (df_数据汇总.货车数 > 500))]
    T_20天与500筛选 = pd.DataFrame(T_20天与500筛选, columns=["站点名称", "地市", "区县", "理应在线天数", "实际在线天数", "在线率", "货车数", "超限10%除外数",
                                                     "超限10%除外超限率(%)"])
    T_20天与500筛选.rename(columns={'理应在线天数': '应在线天数', '超限10%除外数': '超限数', '超限10%除外超限率(%)': '超限率'}, inplace=True)
    T_20天与500筛选 = T_20天与500筛选.sort_values(by="超限率", ascending=False, ignore_index=True)
    print(T_20天与500筛选)

    '''区县超限率排名'''
    T_20天与500筛选 = pd.DataFrame(T_20天与500筛选, columns=["地市", "区县", "货车数", "超限数"])
    区县超限率排序 = T_20天与500筛选.groupby([T_20天与500筛选['地市'], T_20天与500筛选['区县']]).sum()
    区县超限率排序['超限率'] = 区县超限率排序.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
    区县超限率排序 = 区县超限率排序.sort_values('超限率', ascending=False)
    print(区县超限率排序)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/省级月报/超限率相关统计表.xlsx')as writer1:
        区县超限率排序.to_excel(writer1, sheet_name='区县超限率排序', index=True)
        T_20天与500筛选.to_excel(writer1, sheet_name='在线天数大于等于20天货车数大于500的站点数据', index=True)


elif cs =='杭州':
    """附件1"""
    df_区县排列 = pd.read_excel("C:/Users/Administrator/Desktop/输出报表/{}市月报表/{}市区县模板.xlsx".format(cs, cs))
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    df_报修点位统计 = df_报修点位统计[df_报修点位统计.地市 == '{}'.format(cs)]
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    超限数 = df_数据汇总.groupby(['区县'])['超限10%除外数'].sum()
    df_sheet1 = pd.merge(货车数, 超限数, how='left', on='区县')
    df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限10%除外数'] / x['货车数'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
    df_sheet1['超限率排名'] = df_sheet1['超限率'].rank(ascending=False, method='first')
    df_sheet1 = pd.merge(df_区县排列, df_sheet1, on='区县', how='left')
    df_sheet1.rename(columns={'超限10%除外数': '超限数'}, inplace=True)
    print(df_sheet1)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/杭州市月报表/杭州市公路超限率统计表.xlsx')as writer1:
        df_sheet1.to_excel(writer1, sheet_name='附件1', index=True)


elif cs =='湖州':
    """湖州附件1"""
    df_区县排列 = pd.read_excel("C:/Users/Administrator/Desktop/输出报表/{}市月报表/{}市区县模板.xlsx".format(cs, cs))
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    df_报修点位统计 = df_报修点位统计[df_报修点位统计.地市 == '{}'.format(cs)]
    在用数 = df_数据汇总.groupby(['区县'])['站点名称'].count()
    报修数 = df_报修点位统计.groupby(['区县'])['报修站点名称'].count()
    实际站点数 = pd.merge(在用数, 报修数, on='区县', how='left')
    实际站点数 = 实际站点数.fillna(0, inplace=False)
    实际站点数['站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
    实际站点数.站点数 = 实际站点数.站点数.astype(int)
    实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
    实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '报修数'}, inplace=True)
    T_10筛选 = df_数据汇总[(df_数据汇总.实际在线天数 < 10)
                     & (df_数据汇总.实际在线天数 > 0)
                     ]
    T_10筛选 = T_10筛选.groupby([T_10筛选.区县]).count()
    T_10筛选 = T_10筛选.loc[:, ['站点名称']]
    T_10筛选.columns = ['站点名称']
    T_500筛选 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                       & (df_数据汇总.货车数 < 500))]
    T_500筛选 = T_500筛选.groupby([T_500筛选.区县]).count()
    T_500筛选 = T_500筛选.loc[:, ['站点名称']]
    T_500筛选.columns = ['站点名称']
    T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
    T_筛选 = pd.merge(T_10筛选, T_500筛选, how='left', on='区县')
    T_筛选 = T_筛选.fillna(value=0)
    T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
    T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
    t = df_数据汇总[(df_数据汇总.实际在线天数 == 0)]
    T_数据为0数 = t.groupby([t['区县']]).count()
    T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
    T_数据为0数.columns = ['实际在线天数']
    T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
    附件7 = pd.merge(df_区县排列, 实际站点数, how='left', on='区县')
    附件7 = pd.merge(附件7, T_数据为0数, how='left', on='区县')
    附件7 = pd.merge(附件7, T_10或500, how='left', on='区县')
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['异常数'] = 附件7.apply(lambda x: x['报修数'] + x['数据为0'] + x['在线天数＜10天或货车数＜500辆次'], axis=1)
    附件7['异常数'] = 附件7['异常数'].astype('float')
    附件7['站点数'] = 附件7['站点数'].astype('float')
    附件7['完好率'] = 附件7['异常数'] / 附件7['站点数']
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['完好率'] = 附件7['完好率'].apply(lambda x: format(x, '.2%'))
    附件7 = pd.DataFrame(附件7, columns=["区县", "站点数", "报修数", "异常数", "完好率"])
    站点数 = pd.DataFrame(附件7, columns=["区县", "站点数"])
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    超限数 = df_数据汇总.groupby(['区县'])['超限10%除外数'].sum()
    湖州附件1 = pd.merge(站点数, 货车数, how='left', on='区县')
    湖州附件1 = pd.merge(湖州附件1, 超限数, how='left', on='区县')
    湖州附件1['超限率'] = 湖州附件1.apply(lambda x: x['超限10%除外数'] / x['货车数'], axis=1).round(4)
    湖州附件1 = 湖州附件1.fillna(0, inplace=False)
    湖州附件1['超限率'] = 湖州附件1['超限率'].apply(lambda x: format(x, '.2%'))
    湖州附件1 = pd.merge(df_区县排列, 湖州附件1, on='区县', how='left')
    湖州附件1.rename(columns={'超限10%除外数': '超限数'}, inplace=True)
    print(湖州附件1)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/湖州市月报表/湖州市公路超限率统计表.xlsx')as writer1:
        湖州附件1.to_excel(writer1, sheet_name='附件1', index=True)

else:
    """附件1"""
    df_数据汇总 = df_数据汇总[(df_数据汇总.地市 == '{}'.format(cs))]
    df_报修点位统计 = df_报修点位统计[df_报修点位统计.地市 == '{}'.format(cs)]
    货车数 = df_数据汇总.groupby(['区县'])['货车数'].sum()
    超限数 = df_数据汇总.groupby(['区县'])['超限10%除外数'].sum()
    df_sheet1 = pd.merge(货车数, 超限数, how='left', on='区县')
    df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限10%除外数'] / x['货车数'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
    df_sheet1['超限率排名'] = df_sheet1['超限率'].rank(ascending=False, method='first')
    df_sheet1.rename(columns={'超限10%除外数': '超限数'}, inplace=True)
    print(df_sheet1)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/其他市月报表/{}市公路超限率统计表.xlsx'.format(cs))as writer1:
        df_sheet1.to_excel(writer1, sheet_name='附件1', index=True)