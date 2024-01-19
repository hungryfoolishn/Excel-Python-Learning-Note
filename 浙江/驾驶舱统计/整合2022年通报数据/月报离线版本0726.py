# coding: utf-8


import pandas as pd
import numpy as np
import time



file_name = r"C:\Users\stayhungary\Desktop\0828testv1.0.xlsx"

def data_source():

        df_数据汇总 = pd.read_excel(file_name,sheet_name='on_line')
        df_数据汇总.columns = df_数据汇总.iloc[0]
        df_数据汇总 = df_数据汇总.iloc[2:].reset_index(drop=True)
        df_数据汇总 = df_数据汇总[(df_数据汇总.站点名称 != '（经七路）江南路方向K0+200')]
        df_数据汇总.loc[(df_数据汇总['区县'] == '诸暨'), '货车数'] = df_数据汇总[
            '货车数'].map(lambda x: float(x) * 1.4).round(0)
        df_数据汇总.loc[(df_数据汇总['区县'] == '诸暨'), '超限20%除外数'] = df_数据汇总[
            '超限20%除外数'].map(lambda x: float(x) * 0.4).round(0)
        df_数据汇总.loc[df_数据汇总['货车数'] == 0, '货车数'] = 1
        df_数据汇总['超限20%除外数']= df_数据汇总['超限20%除外数'].astype('int')
        df_数据汇总['实际在线天数'] = df_数据汇总['实际在线天数'].astype('int')
        df_数据汇总['货车数'] = df_数据汇总['货车数'].astype('int')
        df_数据汇总['超限20%除外超限率(%)'] = (df_数据汇总['超限20%除外数'] / df_数据汇总['货车数'] * 100).round(2)
        df_数据汇总.loc[df_数据汇总['货车数'] == 1, '货车数'] = 0



        df_报修点位统计 = pd.read_excel(file_name,sheet_name='off_line')
        df_报修点位统计.columns = df_报修点位统计.iloc[0]
        df_报修点位统计 = df_报修点位统计.iloc[2:].reset_index(drop=True)

        df_接入数 = pd.read_excel(file_name,sheet_name='接入数')


        df_案件 = pd.read_excel(file_name,sheet_name='case')
        df_案件.columns = df_案件.iloc[0]
        df_案件 = df_案件.iloc[2:].reset_index(drop=True)


        df_源头 = pd.read_excel(file_name,sheet_name='source')
        df_源头.columns = df_源头.iloc[0]
        df_源头 = df_源头.iloc[2:].reset_index(drop=True)


        df_100t = pd.read_excel(file_name,sheet_name='100t')
        df_100t.columns = df_100t.iloc[0]
        df_100t = df_100t.iloc[2:].reset_index(drop=True)

        df_100= pd.read_excel(file_name,sheet_name='100%')
        df_100.columns = df_100.iloc[0]
        df_100 = df_100.iloc[2:].reset_index(drop=True)

        df_80t= pd.read_excel(file_name,sheet_name='80t')
        df_80t.columns = df_80t.iloc[0]
        df_80t = df_80t.iloc[2:].reset_index(drop=True)
        return df_数据汇总,df_报修点位统计,df_接入数,df_案件,df_源头,df_100t,df_100,df_80t

def data_station():
        df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()


        T_义乌_数据为0 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                            & (df_数据汇总.实际在线天数 == 0)
                            ]
        T_义乌_数据为0数 = T_义乌_数据为0.区县.count()
        T_义乌_汇总 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                          & (df_数据汇总.实际在线天数 < 10)
                          & (df_数据汇总.实际在线天数 > 0)]
        T_义乌_10数 = T_义乌_汇总.区县.count()
        T_义乌_汇总 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                          & (df_数据汇总.实际在线天数 >= 10)
                          & (df_数据汇总.货车数 < 500)
                          ]
        T_义乌_500数 = T_义乌_汇总.区县.count()
        T_义乌_10_500数 = T_义乌_10数 + T_义乌_500数
        T_义乌_报修 = df_报修点位统计[df_报修点位统计.区县 == '义乌']
        T_义乌_报修数 = T_义乌_报修.区县.count()
        T_义乌_在线 = df_数据汇总[df_数据汇总.区县 == '义乌']
        T_义乌_在线数 = T_义乌_在线.区县.count()
        df_数据汇总1 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        在用数 = df_数据汇总1.groupby(['地市'])['站点名称'].count()
        df_报修点位统计1 = df_报修点位统计[df_报修点位统计.区县 != '义乌']
        报修数 = df_报修点位统计1.groupby(['地市'])['报修站点名称'].count()
        实际站点数 = pd.merge(在用数, 报修数, on='地市', how='outer')
        实际站点数 = 实际站点数.fillna(0, inplace=False)
        实际站点数['实际站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
        实际站点数.实际站点数 = 实际站点数.实际站点数.astype(int)
        实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
        实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '实际报修数'}, inplace=True)
        T_10筛选 = df_数据汇总1[(df_数据汇总1.实际在线天数 < 10)
                          & (df_数据汇总1.实际在线天数 > 0)
                          ]
        T_10筛选 = T_10筛选.groupby([T_10筛选.地市]).count()
        T_10筛选 = T_10筛选.loc[:, ['站点名称']]
        T_10筛选.columns = ['站点名称']
        T_500筛选 = df_数据汇总1[((df_数据汇总1.实际在线天数 >= 10)
                            & (df_数据汇总1.货车数 < 500))]
        T_500筛选 = T_500筛选.groupby([T_500筛选.地市]).count()
        T_500筛选 = T_500筛选.loc[:, ['站点名称']]
        T_500筛选.columns = ['站点名称']
        T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
        T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on='地市')
        T_筛选 = T_筛选.fillna(value=0)
        T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
        T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
        t = df_数据汇总1[(df_数据汇总1.实际在线天数 == 0)]
        T_数据为0数 = t.groupby([t['地市']]).count()
        T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
        T_数据为0数.columns = ['实际在线天数']
        T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
        站点设备完好率 = pd.merge(df_接入数, 实际站点数, on='地市', how='left')
        站点设备完好率.loc[11, '在用数'] = T_义乌_在线数
        站点设备完好率.loc[11, '实际报修数'] = T_义乌_报修数
        站点设备完好率.loc[11, '实际站点数'] = T_义乌_报修数 + T_义乌_在线数
        站点设备完好率 = 站点设备完好率.fillna(0, inplace=False)
        站点设备完好率['报修数'] = 站点设备完好率.apply(lambda x: x['接入数（修正后）'] - x['在用数'], axis=1)
        站点设备完好率.loc[站点设备完好率['报修数'] < 0, '报修数'] = 0
        站点设备完好率 = pd.merge(站点设备完好率, T_数据为0数, on='地市', how='left')
        站点设备完好率 = pd.merge(站点设备完好率, T_10或500, on='地市', how='left')
        站点设备完好率.loc[11, '数据为0'] = T_义乌_数据为0数
        站点设备完好率.loc[11, '在线天数＜10天或货车数＜500辆次'] = T_义乌_10_500数
        站点设备完好率 = 站点设备完好率.fillna(0, inplace=False)

        站点设备完好率['实际完好数'] = 站点设备完好率['在用数'] - 站点设备完好率.数据为0 - 站点设备完好率['在线天数＜10天或货车数＜500辆次']
        站点设备完好率['修正完好数'] = 站点设备完好率.apply(lambda x: min(x['接入数（修正后）'], x['实际完好数']), axis=1)
        站点设备完好率['实际完好率'] = 站点设备完好率.apply(lambda x: x['实际完好数'] / x['实际站点数'], axis=1).round(4)
        站点设备完好率['实际完好率'] = 站点设备完好率['实际完好率'].apply(lambda x: format(x, '.2%'))
        站点设备完好率['调整后完好率'] = 站点设备完好率.apply(lambda x: x['修正完好数'] / x['接入数（修正后）'], axis=1).round(4)
        站点设备完好率['调整后完好率'] = 站点设备完好率['调整后完好率'].apply(lambda x: format(x, '.2%'))
        站点设备完好率 = pd.DataFrame(站点设备完好率,
                               columns=["地市", "接入数（修正后）", "实际站点数", "在用数", "实际报修数", "报修数", "数据为0", "在线天数＜10天或货车数＜500辆次",
                                        "实际完好数", "实际完好率", "修正完好数", "调整后完好率"])

        df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()
        """做sheet1"""

        df_数据汇总 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                           & (df_数据汇总.货车数 > 500))]
        T_义乌 = df_数据汇总[(df_数据汇总['区县'] == '义乌')]
        T_义乌_货车数 = T_义乌.groupby(['区县'])['货车数'].sum()
        T_义乌_超限数 = T_义乌.groupby(['区县'])['超限20%除外数'].sum()
        df_数据汇总1 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        货车数 = df_数据汇总1.groupby(['地市'])['货车数'].sum()
        超限数 = df_数据汇总1.groupby(['地市'])['超限20%除外数'].sum()
        df_sheet1 = pd.merge(货车数, 超限数, how='left', on='地市')
        df_sheet1 = pd.merge(df_接入数, df_sheet1, how='left', on='地市')
        df_sheet1.loc[11, '货车数'] = T_义乌_货车数[0]
        df_sheet1.loc[11, '超限20%除外数'] = T_义乌_超限数[0]
        df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限20%除外数'] / x['货车数'], axis=1).round(4)
        df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
        df_sheet1['超限率排名'] = df_sheet1['超限率'].rank(ascending=False, method='first')
        df_sheet1后 = pd.DataFrame(站点设备完好率, columns=["地市", "实际站点数", "实际完好率", "调整后完好率"])
        df_sheet1 = pd.merge(df_sheet1, df_sheet1后, how='left', on='地市')
        df_sheet1 = pd.DataFrame(df_sheet1,
                                 columns=["地市", "货车数", "超限20%除外数", "超限率", "超限率排名", "实际站点数", "接入数（修正后）", "实际完好率",
                                          "调整后完好率"])
        df_sheet1.rename(columns={'超限20%除外数': '超限数'}, inplace=True)

        """在线天数大于等于10天货车数大于500的站点数据"""

        T_20天与500筛选 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                               & (df_数据汇总.货车数 > 500))]
        T_20天与500筛选 = pd.DataFrame(T_20天与500筛选,
                                   columns=["站点名称", "地市", "区县", "理应在线天数", "实际在线天数", "在线率", "货车数", "超限20%除外数",
                                            "超限20%除外超限率(%)"])
        T_20天与500筛选.rename(columns={'理应在线天数': '应在线天数', '超限20%除外数': '超限数', '超限20%除外超限率(%)': '超限率'}, inplace=True)
        T_20天与500筛选1 = T_20天与500筛选.sort_values(by="超限率", ascending=False, ignore_index=True)

        '''区县超限率排名'''
        T_20天与500筛选 = pd.DataFrame(T_20天与500筛选1, columns=["地市", "区县", "货车数", "超限数"])
        区县超限率排序 = T_20天与500筛选.groupby([T_20天与500筛选['地市'], T_20天与500筛选['区县']]).sum()
        区县超限率排序['超限率'] = 区县超限率排序.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
        区县超限率排序 = 区县超限率排序.sort_values('超限率', ascending=False)
        区县超限率排序['超限率'] = 区县超限率排序['超限率'].apply(lambda x: format(x, '.2%'))
        区县超限率排序 = 区县超限率排序.reset_index()
        df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()
        return df_数据汇总, df_报修点位统计, df_sheet1, 站点设备完好率, 区县超限率排序, T_20天与500筛选1


def overrun_site_rate():
    """附件1"""
    df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()
    df_数据汇总2 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                        & (df_数据汇总.货车数 > 500))]
    货车数 = df_数据汇总2.groupby(['地市', '区县'])['货车数'].sum().reset_index()
    超限数 = df_数据汇总2.groupby(['地市', '区县'])['超限20%除外数'].sum().reset_index()
    df_sheet1 = pd.merge(货车数, 超限数, how='left', on=['地市', '区县'])
    df_sheet1.loc[df_sheet1['货车数'] == 0, '货车数'] = 1
    df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限20%除外数'] / x['货车数'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
    df_sheet1.loc[df_sheet1['货车数'] == 1, '货车数'] = 0
    df_sheet1.rename(columns={'超限20%除外数': '超限数'}, inplace=True)

    """附件7"""

    df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()
    # df_数据汇总.columns = df_数据汇总.iloc[2]
    # df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
    在用数 = df_数据汇总.groupby(['地市', '区县'])['站点名称'].count()
    报修数 = df_报修点位统计.groupby(['地市', '区县'])['报修站点名称'].count()
    实际站点数 = pd.merge(在用数, 报修数, on=['地市', '区县'], how='outer')
    实际站点数 = 实际站点数.fillna(0, inplace=False)
    实际站点数['站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
    实际站点数.站点数 = 实际站点数.站点数.astype(int)
    实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
    实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '报修数'}, inplace=True)

    T_10筛选 = df_数据汇总[(df_数据汇总.实际在线天数 < 10)
                     & (df_数据汇总.实际在线天数 > 0)
                     ]
    T_10筛选 = T_10筛选.groupby(['地市', '区县']).count()
    T_10筛选 = T_10筛选.loc[:, ['站点名称']]
    T_10筛选.columns = ['站点名称']
    T_500筛选 = df_数据汇总[(df_数据汇总.实际在线天数 >= 10) & (df_数据汇总.货车数 < 500)]
    T_500筛选 = T_500筛选.groupby(['地市', '区县']).count()
    T_500筛选 = T_500筛选.loc[:, ['站点名称']]
    T_500筛选.columns = ['站点名称']
    T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
    T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on=['地市', '区县'])
    T_筛选 = T_筛选.fillna(value=0)
    T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
    T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
    t = df_数据汇总[(df_数据汇总.实际在线天数 == 0)]
    T_数据为0数 = t.groupby(['地市', '区县']).count()

    T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
    T_数据为0数.columns = ['实际在线天数']
    T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
    附件7 = pd.merge(实际站点数, T_数据为0数, how='left', on=['地市', '区县'])
    附件7 = pd.merge(附件7, T_10或500, how='left', on=['地市', '区县'])
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['异常数'] = 附件7.apply(lambda x: x['报修数'] + x['数据为0'] + x['在线天数＜10天或货车数＜500辆次'], axis=1)
    附件7['异常数'] = 附件7['异常数'].astype('float')
    附件7['站点数'] = 附件7['站点数'].astype('float')
    附件7['设备完好率'] = (附件7['站点数'] - 附件7['报修数']) / 附件7['站点数']
    附件7['数据完好率'] = (附件7['站点数'] - 附件7['异常数']) / 附件7['站点数']
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['数据完好率'] = 附件7['数据完好率'].apply(lambda x: format(x, '.2%'))
    附件7['设备完好率'] = 附件7['设备完好率'].apply(lambda x: format(x, '.2%'))
    附件7 = pd.DataFrame(附件7, columns=["站点数", "报修数",'设备完好率', "异常数", "数据完好率"]).reset_index()
    附件7 = pd.merge(df_sheet1, 附件7, how='outer', on=['地市', '区县'])
    return 附件7
    # q = input("请输入存储路径(C:/Users/Administrator/Desktop/输出报表/其他市月报表)：")


def over_100_num():
        df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t=data_source()
        station_code = df_数据汇总['站点名称']
        U_过车_站点表 = df_100[df_100.loc[:, '站点名称'].isin(station_code)]

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
            "330122": 1.091,
            "330183": 1.1,
            "330329": 1.2,
            "330523": 1.03,
            "330603": 1.1,
            "330604": 1.2,
            "330624": 1.2,
            "330681": 1.1,
            "330703": 1.2,
            "330782": 1.1
        }
        U_过车_站点表['总重'] = U_过车_站点表['总重'].astype('float')
        U_过车_站点表['超重'] = U_过车_站点表['超重'].astype('float')
        U_过车_站点表['limit_weight'] = U_过车_站点表['总重'] - U_过车_站点表['超重']
        U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
        U_过车_站点表.loc[U_过车_站点表['limit_weight'] < 0, 'limit_weight'] = 0.0001
        for item in i.items():
            key = item[0]
            value = item[1]

            U_过车_站点表.loc[((U_过车_站点表['地市编码'] == key) | (U_过车_站点表['区县编码'] == key)) & (
                    U_过车_站点表['总重'] < 100), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(
                lambda x: float(x) * value).round(4)
        U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
        U_过车_站点表['总重'] = U_过车_站点表['总重'].astype('float')
        U_过车_站点表['vehicle_brand'] = U_过车_站点表['vehicle_brand'].astype('float')
        U_过车_站点表['超限率100%'] = U_过车_站点表.apply(
            lambda x: (x['总重'] - x['vehicle_brand']) - x['vehicle_brand'],
            axis=1).round(2)
        U_过车_站点表 = U_过车_站点表[(U_过车_站点表['超限率100%'] >= 0)
        ]
        U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                                columns=["地市", "区县", "车牌号码", "检测时间", "轴数", "总重",
                                         "超重",
                                         "超限率", "站点名称", "状态", "是否是需采集的数据", "异常数据",
                                         "流水号",
                                         "photo1", "photo2", "photo3", "vedio", '区县编码'])


        超限100数 = U_过车_站点表.groupby(["地市", "区县"])['流水号'].count().reset_index(name='超限100')

        U_过车_站点表 = U_过车_站点表.sort_values(by=['区县编码', '地市', '区县', '检测时间'],
                                        ascending=True).reset_index(drop=True)
        U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['流水号'])
        U_过车_站点表['状态'] = U_过车_站点表['状态'].astype('int')
        U_过车_站点表['是否是需采集的数据'] = U_过车_站点表['是否是需采集的数据'].astype('int')
        U_过车_站点表.loc[U_过车_站点表['状态'] == 1, '状态'] = '待初审'
        U_过车_站点表.loc[U_过车_站点表['状态'] == 2, '状态'] = '待审核'
        U_过车_站点表.loc[U_过车_站点表['状态'] == 9, '状态'] = '判定不处理'
        U_过车_站点表.loc[U_过车_站点表['状态'] == 15, '状态'] = '初审不通过'
        U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 1, '是否是需采集的数据'] = '满足'
        U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 0, '是否是需采集的数据'] = '不满足'

        """车牌处理"""
        U_过车_站点表['车牌号码'].fillna('无牌', inplace=True)
        U_过车_站点表['字节数'] = U_过车_站点表['车牌号码'].str.len()
        U_过车_站点表.loc[U_过车_站点表['字节数'] <= 5, '车牌号码'] = '无牌'
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo1'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo2'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo3'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'vedio'] = ''

        """地市货车数及超限100%数"""
        df_数据汇总2 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                            & (df_数据汇总.货车数 > 500))]
        货车数 = df_数据汇总2.groupby(['地市'])['货车数'].sum()
        货车数 = 货车数.to_frame()
        货车数 = 货车数.reset_index()
        超限100 = U_过车_站点表.groupby(['地市'])['流水号'].count()
        超限100 = 超限100.to_frame()
        超限100 = 超限100.reset_index()
        附件2 = pd.merge(货车数, 超限100, left_on='地市', right_on='地市', how='left', )
        附件2.rename(columns={'流水号': '超限100%数'}, inplace=True)
        附件2 = 附件2.fillna(0, inplace=False)
        附件2['超限100%数占货车数比例'] = 附件2.apply(lambda x: x['超限100%数'] / x['货车数'], axis=1)
        附件2['排名（占比由高到低）'] = 附件2['超限100%数占货车数比例'].rank(ascending=False, method='first')
        附件2['超限100%数占货车数比例'] = 附件2['超限100%数占货车数比例'].apply(lambda x: format(x, '.3%'))
        未识别到车牌数 = U_过车_站点表[U_过车_站点表['车牌号码'] == '无牌'].groupby(['地市'])['车牌号码'].count().reset_index(name='未识别到车牌')
        满足证据条件数 = U_过车_站点表[((U_过车_站点表['车牌号码'] == '无牌') & (U_过车_站点表['是否是需采集的数据'] == '满足'))].groupby(['地市'])[
            '车牌号码'].count().reset_index(name='满足证据条件')
        附件2 = pd.merge(df_接入数, 附件2, on='地市', how='left')
        附件2 = pd.merge(附件2, 未识别到车牌数, left_on='地市', right_on='地市', how='left')
        附件2 = pd.merge(附件2, 满足证据条件数, left_on='地市', right_on='地市', how='left')
        附件2['满足证据条件且故意遮挡车牌'] = 0
        附件2 = pd.DataFrame(附件2, columns=['地市', '货车数', '超限100%数', '超限100%数占货车数比例', '排名（占比由高到低）', '未识别到车牌', '满足证据条件',
                                         '满足证据条件且故意遮挡车牌'])
        附件2 = 附件2.fillna(0, inplace=False)
        附件2 = 附件2.drop(index=(附件2.loc[(附件2['地市'] == '义乌')].index))

        return 超限100数,附件2,U_过车_站点表

def total_weight_80_90():
    df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t = data_source()
    station_name = df_数据汇总['站点名称']
    df_big = df_80t[df_80t.loc[:, '站点名称'].isin(station_name)]
    总重80吨以上数 = df_big.groupby(['区县编码'])['流水号'].count().reset_index(name='本月超限80吨以上')
    df_big['总重'] = df_big['总重'].astype('float')
    总重90吨以上数 = df_big[df_big['总重']>90].groupby(['区县编码'])['流水号'].count().reset_index(name='本月超限90吨以上')
    总重80_90以上 = pd.merge(总重80吨以上数, 总重90吨以上数, on=['区县编码'],how='left')
    df_big = pd.DataFrame(df_big,
                            columns=["地市", "区县", "站点名称", "检测时间","车牌号码", "总重","限重",
                                     "超重","轴数", "超限率", "流水号",  "状态"])
    df_big['总重'] = df_big['总重'].astype('float')
    return 总重80_90以上,df_big

def total_weight_100_num():
    df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t = data_source()
    station_name = df_数据汇总['站点名称']
    df_100t = df_100t[df_100t.loc[:, '站点名称'].isin(station_name)]
    U_过车_站点表 = pd.DataFrame(df_100t,
                            columns=["地市", "区县", "车牌号码", "检测时间", "轴数", "总重",
                                     "超重",
                                     "超限率", "站点名称", "状态", "是否是需采集的数据", "异常数据",
                                     "流水号",
                                     "photo1", "photo2", "photo3", "vedio", '区县编码'])
    U_过车_站点表 = U_过车_站点表.sort_values(by=['区县编码', '地市', '区县', '检测时间'],
                                    ascending=True).reset_index(drop=True)
    U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['流水号'])
    U_过车_站点表['状态'] = U_过车_站点表['状态'].astype('int')
    U_过车_站点表['是否是需采集的数据'] = U_过车_站点表['是否是需采集的数据'].astype('int')
    U_过车_站点表.loc[U_过车_站点表['状态'] == 1, '状态'] = '待初审'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 2, '状态'] = '待审核'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 9, '状态'] = '判定不处理'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 15, '状态'] = '初审不通过'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 1, '是否是需采集的数据'] = '满足'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 0, '是否是需采集的数据'] = '不满足'

    U_过车_站点表['区县编码'] = U_过车_站点表['区县编码'].astype('string')
    百吨王数 = U_过车_站点表.groupby(['区县编码'])['流水号'].count().reset_index(name='百吨王数')

    return  百吨王数,U_过车_站点表


if __name__ == "__main__":
    df_区县编码 = pd.read_excel(file_name, sheet_name='code_area')
    df_区县编码['区县编码'] = df_区县编码['区县编码'].astype('string')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    df_源头站点明细 = pd.read_excel(file_name, sheet_name='源头站点明细')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    df_数据汇总, df_报修点位统计, df_接入数, df_案件, df_源头, df_100t, df_100, df_80t = data_source()
    df_数据汇总, df_报修点位统计,df_sheet1,站点设备完好率,区县超限率排序,T_20天与500筛选1 = data_station()
    超限100数,超限100汇总,超限100明细=over_100_num()
    总重80_90以上,总重80吨以上明细 =total_weight_80_90()
    百吨王数, 百吨王明细 = total_weight_100_num()
    附件7 = overrun_site_rate()
    附件7 = pd.merge(df_区县编码, 附件7, on=['地市', '区县'], how='outer')
    附件7 = pd.merge(附件7, 超限100数, on=['地市','区县'],how='left')
    附件7 = pd.merge(附件7, 总重80_90以上, on=['区县编码'], how='left')
    附件7 = pd.merge(附件7, 百吨王数, on=['区县编码'], how='left')
    附件7 = pd.merge(附件7, df_案件, on=['地市编码','地市','区县编码','区县'], how='outer')
    df_源头=df_源头.drop(['统计月份'],axis=1)
    附件7 = pd.merge(附件7, df_源头, on=['地市编码','地市','区县编码','区县'], how='outer')
    附件7['超限100%数货车数/万辆'] = 附件7.apply(lambda x: x['超限100'] / (x['货车数'] + 0.0000001)*10000, axis=1).round(4)
    附件7['源头货车数'] = pd.to_numeric(附件7['源头货车数'], errors='coerce')
    附件7['数据站点总数'] = pd.to_numeric(附件7['数据站点总数'], errors='coerce')
    附件7['在线站点数'] = pd.to_numeric(附件7['在线站点数'], errors='coerce')
    附件7['20-50%数'] = pd.to_numeric(附件7['20-50%数'], errors='coerce')
    附件7['50-100%数'] = pd.to_numeric(附件7['50-100%数'], errors='coerce')
    附件7['100%以上数'] = pd.to_numeric(附件7['100%以上数'], errors='coerce')
    附件7['源头单位平均过车数（辆次）'] = 附件7.apply(lambda x: x['源头货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    附件7['设备上线率（%）'] = 附件7.apply(lambda x: x['在线站点数'] / (x['数据站点总数']+ 0.0000001), axis=1)
    附件7['20-50%占比'] = 附件7.apply(lambda x: x['20-50%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7['50-100%占比'] = 附件7.apply(lambda x: x['50-100%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7['100%以上占比'] = 附件7.apply(lambda x: x['100%以上数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['设备上线率（%）'] = 附件7['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    附件7['20-50%占比'] = 附件7['20-50%占比'].apply(lambda x: format(x, '.2%'))
    附件7['50-100%占比'] = 附件7['50-100%占比'].apply(lambda x: format(x, '.2%'))
    附件7['100%以上占比'] = 附件7['100%以上占比'].apply(lambda x: format(x, '.2%'))
    附件7['80吨以上总数'] =附件7['本月超限80吨以上']
    附件7['入库数(路政)'] = 附件7['入库数(路政)'].astype('int')
    附件7['本省入库数'] = 附件7['本省入库数'].astype('int')
    附件7['非现场处罚(路政)'] = 附件7['非现场处罚(路政)'].astype('int')
    附件7['非现场处罚本省(路政)'] = 附件7['非现场处罚本省(路政)'].astype('int')
    附件7['外省入库数'] = 附件7['入库数(路政)']-附件7['本省入库数']
    附件7['非现场处罚外省(路政)'] = 附件7['非现场处罚(路政)'] - 附件7['非现场处罚本省(路政)']
    附件7 = pd.DataFrame(附件7, columns=['统计月份', '地市编码','地市','区县编码','区县','货车数','超限数','超限率','超限100','超限100%数货车数/万辆',
                                     '本月超限80吨以上','本月超限90吨以上','百吨王数','超限100%遮挡车牌数量（辆）', '百吨王遮挡车牌数量（辆）', '站点数',
                                     '报修数','设备完好率','异常数','数据完好率','入库数(路政)','本省入库数','外省入库数','现场处罚(路政)','非现场处罚(路政)','非现场处罚本省(路政)','非现场处罚(路政)当年',
                                     '非现场处罚本省(路政)当年','非现场处罚外省(路政)','交警现场查处数','交警非现场处罚数','交警非现查处数本省','需处罚数/非现入库数（总计）','需处罚数/非现入库数（本省）',
                                     '需处罚数/非现入库数（外省）','非现处罚数（总计）','非现处罚数（本省）','非现处罚数（外省）','年处罚数','外省抄告','非现场处罚率（本省）','非现场处罚率（外省）',
                                     '处罚率(含抄告）','80吨以上总数','80吨以上且满足','80吨以上且审核通过', '合规率','后面是源头数据','数据站点总数','源头货车数','源头单位平均过车数（辆次）',
                                     '在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比'])
    附件7['区县编码(路政)'] = 附件7['区县编码'].astype('int')
    附件7 = 附件7.sort_values(by=['区县编码'],
                                    ascending=True).reset_index(drop=True)
    附件7 = 附件7.fillna(0, inplace=False)
    附件7地市 = 附件7.groupby(['地市编码','地市']).sum().reset_index()
    附件7地市.loc[附件7地市['货车数'] == 0, '货车数'] = 1
    附件7地市['超限率'] = 附件7地市.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['超限率'] = 附件7地市['超限率'].apply(lambda x: format(x, '.2%'))
    附件7地市.loc[附件7地市['货车数'] == 1, '货车数'] = 0
    附件7地市['设备完好率'] = (附件7地市['站点数'] - 附件7地市['报修数']) / 附件7地市['站点数']
    附件7地市['数据完好率'] = (附件7地市['站点数'] - 附件7地市['异常数']) / 附件7地市['站点数']
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['数据完好率'] = 附件7地市['数据完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市['设备完好率'] = 附件7地市['设备完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市['超限100%数货车数/万辆'] = 附件7地市.apply(lambda x: x['超限100'] / (x['货车数'] + 0.0000001)*10000, axis=1).round(4)
    附件7地市['源头货车数'] = pd.to_numeric(附件7地市['源头货车数'], errors='coerce')
    附件7地市['数据站点总数'] = pd.to_numeric(附件7地市['数据站点总数'], errors='coerce')
    附件7地市['在线站点数'] = pd.to_numeric(附件7地市['在线站点数'], errors='coerce')
    附件7地市['20-50%数'] = pd.to_numeric(附件7地市['20-50%数'], errors='coerce')
    附件7地市['50-100%数'] = pd.to_numeric(附件7地市['50-100%数'], errors='coerce')
    附件7地市['100%以上数'] = pd.to_numeric(附件7地市['100%以上数'], errors='coerce')
    附件7地市['源头单位平均过车数（辆次）'] = 附件7地市.apply(lambda x: x['源头货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    附件7地市['设备上线率（%）'] = 附件7地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']+ 0.0000001), axis=1)
    附件7地市['20-50%占比'] = 附件7地市.apply(lambda x: x['20-50%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市['50-100%占比'] = 附件7地市.apply(lambda x: x['50-100%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市['100%以上占比'] = 附件7地市.apply(lambda x: x['100%以上数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['设备上线率（%）'] = 附件7地市['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    附件7地市['20-50%占比'] = 附件7地市['20-50%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['50-100%占比'] = 附件7地市['50-100%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['100%以上占比'] = 附件7地市['100%以上占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['80吨以上总数'] =附件7地市['本月超限80吨以上']
    附件7地市['外省入库数'] = 附件7地市['入库数(路政)']-附件7地市['本省入库数']
    附件7地市['非现场处罚外省(路政)'] = 附件7地市['非现场处罚(路政)'] - 附件7地市['非现场处罚本省(路政)']
    附件7地市['区县'] = 附件7地市['地市']
    附件7地市['区县编码'] = 附件7地市['地市编码']
    附件7地市['统计月份'] = 附件7['统计月份']
    # 附件7地市 = 附件7地市.set_index()
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市 = pd.DataFrame(附件7地市, columns=['统计月份','地市编码','地市','区县编码','区县','货车数','超限数','超限率','超限100','超限100%数货车数/万辆',
                                     '本月超限80吨以上','本月超限90吨以上','百吨王数','超限100%遮挡车牌数量（辆）', '百吨王遮挡车牌数量（辆）', '站点数',
                                     '报修数','设备完好率','异常数','数据完好率','入库数(路政)','本省入库数','外省入库数','现场处罚(路政)','非现场处罚(路政)','非现场处罚本省(路政)','非现场处罚(路政)当年',
                                     '非现场处罚本省(路政)当年','非现场处罚外省(路政)','交警现场查处数','交警非现场处罚数','交警非现查处数本省','需处罚数/非现入库数（总计）','需处罚数/非现入库数（本省）',
                                     '需处罚数/非现入库数（外省）','非现处罚数（总计）','非现处罚数（本省）','非现处罚数（外省）','年处罚数','外省抄告','非现场处罚率（本省）','非现场处罚率（外省）',
                                     '处罚率(含抄告）','80吨以上总数','80吨以上且满足','80吨以上且审核通过', '合规率','后面是源头数据','数据站点总数','源头货车数','源头单位平均过车数（辆次）',
                                     '在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比'])
    附件7地市 = 附件7地市.sort_values(by=['区县编码'],
                          ascending=True).reset_index(drop=True)


    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\test0907.xlsx') as writer1:
        附件7.to_excel(writer1, sheet_name='区县汇总', index=False)
        附件7地市.to_excel(writer1, sheet_name='地市汇总', index=True)
        df_报修点位统计.to_excel(writer1, sheet_name='报修', index=False)
        df_数据汇总.to_excel(writer1, sheet_name='在线')
        df_sheet1.to_excel(writer1, sheet_name='sheet1', index=True)
        站点设备完好率.to_excel(writer1, sheet_name='站点设备完好率', index=True)
        区县超限率排序.to_excel(writer1, sheet_name='区县超限率排序', index=True)
        T_20天与500筛选1.to_excel(writer1, sheet_name='在线天数大于等于20天货车数大于500的站点数据', index=True)
        df_源头站点明细.to_excel(writer1, sheet_name='源头站点数据明细', index=False)
        总重80吨以上明细.to_excel(writer1, sheet_name='总重80_90以上明细')
        超限100汇总.to_excel(writer1, sheet_name='超限100汇总', index=True)
        超限100明细.to_excel(writer1, sheet_name='超限100明细', index=True)
        百吨王明细.to_excel(writer1, sheet_name='百吨王明细')