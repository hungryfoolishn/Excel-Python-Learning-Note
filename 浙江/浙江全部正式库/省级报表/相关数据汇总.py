import pymysql
import pandas as pd
import numpy as np
import datetime as dt
import io


df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)
df_接入数= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/接入数.xlsx")
df_报修点位统计= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")


"""站点完好率"""

df_报修点位统计.columns = df_报修点位统计.iloc[0]
df_报修点位统计= df_报修点位统计.iloc[1:].reset_index(drop=True)
T_义乌_数据为0=df_数据汇总[(df_数据汇总.区县 =='义乌')
                      &(df_数据汇总.实际在线天数== 0)
                      ]
T_义乌_数据为0数=T_义乌_数据为0.区县.count()
T_义乌_汇总=df_数据汇总[(df_数据汇总.区县 =='义乌')
                      &(df_数据汇总.实际在线天数 < 10)
                      &(df_数据汇总.实际在线天数 > 0)]
T_义乌_10数=T_义乌_汇总.区县.count()
T_义乌_汇总=df_数据汇总[(df_数据汇总.区县 =='义乌')
                      &(df_数据汇总.实际在线天数 >= 10)
                      &(df_数据汇总.货车数 < 500)
                      ]
T_义乌_500数=T_义乌_汇总.区县.count()
T_义乌_10_500数=T_义乌_10数+T_义乌_500数
T_义乌_报修=df_报修点位统计[df_报修点位统计.区县=='义乌']
T_义乌_报修数=T_义乌_报修.区县.count()
T_义乌_在线=df_数据汇总[df_数据汇总.区县=='义乌']
T_义乌_在线数=T_义乌_在线.区县.count()
df_数据汇总=df_数据汇总[(df_数据汇总.区县 !='义乌')
                    ]
在用数=df_数据汇总.groupby(['地市'])['站点名称'].count()
df_报修点位统计=df_报修点位统计[df_报修点位统计.区县!='义乌']
报修数=df_报修点位统计.groupby(['地市'])['报修站点名称'].count()
实际站点数=pd.merge(在用数,报修数,on='地市',how='left')
实际站点数=实际站点数.fillna(0,inplace=False)
实际站点数['实际站点数']=实际站点数.apply(lambda x: x[0] + x[1], axis=1)
实际站点数.实际站点数 = 实际站点数.实际站点数 .astype(int)
实际站点数.报修站点名称 = 实际站点数.报修站点名称 .astype(int)
实际站点数.rename(columns={'站点名称':'在用数','报修站点名称':'实际报修数'},inplace=True)
T_10筛选=df_数据汇总[(df_数据汇总.实际在线天数 < 10)
               &(df_数据汇总.实际在线天数 > 0)
                                      ]
T_10筛选=T_10筛选.groupby([T_10筛选.地市]).count()
T_10筛选 = T_10筛选.loc[:,['站点名称']]
T_10筛选.columns =['站点名称']
T_500筛选=df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                      &(df_数据汇总.货车数 < 500)) ]
T_500筛选=T_500筛选.groupby([T_500筛选.地市]).count()
T_500筛选 = T_500筛选.loc[:,['站点名称']]
T_500筛选.columns =['站点名称']
T_500筛选.rename(columns={'站点名称':'500辆次'},inplace=True)
T_筛选=pd.merge(T_10筛选,T_500筛选,how='left',on='地市')
T_筛选=T_筛选.fillna(value=0)
T_筛选['在线天数＜10天或货车数＜500辆次']=T_筛选['站点名称']+T_筛选['500辆次']
T_10或500=T_筛选['在线天数＜10天或货车数＜500辆次']
t=df_数据汇总[(df_数据汇总.实际在线天数==0)]
T_数据为0数= t.groupby([t['地市']]).count()
T_数据为0数 = T_数据为0数.loc[:,['实际在线天数']]
T_数据为0数.columns =['实际在线天数']
T_数据为0数.rename(columns={'实际在线天数':'数据为0'},inplace=True)
站点设备完好率=pd.merge(df_接入数,实际站点数,on='地市',how='left')
站点设备完好率.loc[11,'在用数'] = T_义乌_在线数
站点设备完好率.loc[11,'实际报修数'] = T_义乌_报修数
站点设备完好率.loc[11,'实际站点数'] = T_义乌_报修数+T_义乌_在线数
站点设备完好率=站点设备完好率.fillna(0,inplace=False)
站点设备完好率[['在用数','实际报修数','实际站点数']] = 站点设备完好率[['在用数','实际报修数','实际站点数']].apply(np.int64)
站点设备完好率['报修数']=站点设备完好率.apply(lambda x: x['接入数（修正后）']-x['在用数'], axis= 1)
站点设备完好率.loc[站点设备完好率['报修数'] < 0, '报修数'] = 0
站点设备完好率=pd.merge(站点设备完好率,T_数据为0数,on='地市',how='left')
站点设备完好率=pd.merge(站点设备完好率,T_10或500,on='地市',how='left')
站点设备完好率.loc[11,'数据为0'] = T_义乌_数据为0数
站点设备完好率.loc[11,'在线天数＜10天或货车数＜500辆次'] = T_义乌_10_500数
站点设备完好率=站点设备完好率.fillna(0,inplace=False)
站点设备完好率[['数据为0','在线天数＜10天或货车数＜500辆次']] = 站点设备完好率[['数据为0','在线天数＜10天或货车数＜500辆次']].apply(np.int64)
站点设备完好率['实际完好数']=站点设备完好率['在用数']-站点设备完好率.数据为0-站点设备完好率['在线天数＜10天或货车数＜500辆次']
站点设备完好率['修正完好数']=站点设备完好率.apply(lambda x: min( x['接入数（修正后）'],x['实际完好数']), axis= 1)
站点设备完好率['实际完好率']=站点设备完好率.apply(lambda x: x['实际完好数']/x['实际站点数'], axis= 1).round(4)
站点设备完好率['实际完好率']= 站点设备完好率['实际完好率'].apply(lambda x: format(x, '.2%'))
站点设备完好率['调整后完好率']=站点设备完好率.apply(lambda x:  x['修正完好数']/x['接入数（修正后）'], axis= 1).round(4)
站点设备完好率['调整后完好率']= 站点设备完好率['调整后完好率'].apply(lambda x: format(x, '.2%'))
站点设备完好率=pd.DataFrame(站点设备完好率,columns=["地市","接入数（修正后）","实际站点数","在用数","实际报修数","报修数","数据为0","在线天数＜10天或货车数＜500辆次","实际完好数","实际完好率","修正完好数","调整后完好率"])
print(站点设备完好率)
站点设备完好率.to_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——站点设备完好率.xlsx",sheet_name='站点设备完好率')




"""做sheet1"""

df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)
df_站点设备完好率 = pd.read_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——站点设备完好率.xlsx",sheet_name='站点设备完好率')
df_接入数= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/接入数.xlsx")
T_义乌=df_数据汇总[(df_数据汇总.区县 =='义乌')]
T_义乌_货车数=T_义乌.groupby(['区县'])['货车数'].sum()
T_义乌_超限数=T_义乌.groupby(['区县'])['超限10%除外数'].sum()
df_数据汇总=df_数据汇总[(df_数据汇总.区县 !='义乌')
                    ]
货车数=df_数据汇总.groupby(['地市'])['货车数'].sum()
超限数=df_数据汇总.groupby(['地市'])['超限10%除外数'].sum()
df_sheet1=pd.merge(货车数,超限数,how='left',on='地市')
df_sheet1=pd.merge(df_接入数,df_sheet1,how='left',on='地市')
df_sheet1.loc[11,'货车数'] = T_义乌_货车数[0]
df_sheet1.loc[11,'超限10%除外数'] = T_义乌_超限数[0]
df_sheet1['超限率']=df_sheet1.apply(lambda x: x['超限10%除外数']/x['货车数'], axis=1).round(4)
df_sheet1['超限率']= df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
df_sheet1['超限率排名']=df_sheet1['超限率'].rank(ascending=False, method='first')
df_sheet1后=pd.DataFrame(df_站点设备完好率,columns=["地市","实际站点数","实际完好率","调整后完好率"])
df_sheet1=pd.merge(df_sheet1,df_sheet1后,how='left',on='地市')
df_sheet1=pd.DataFrame(df_sheet1,columns=["地市","货车数","超限10%除外数","超限率","超限率排名","实际站点数","接入数（修正后）","实际完好率","调整后完好率"])
df_sheet1['超限率排名'] = df_sheet1['超限率排名'].apply(np.int64)
df_sheet1.rename(columns={'超限10%除外数':'超限数'},inplace=True)
df_sheet1.to_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——sheet1.xlsx",sheet_name='sheet1')




"""在线天数大于等于20天货车数大于500的站点数据"""

df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)
T_20天与500筛选=df_数据汇总[((df_数据汇总.实际在线天数 >= 20)
                      &(df_数据汇总.货车数 > 500)) ]
T_20天与500筛选=pd.DataFrame(T_20天与500筛选,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限10%除外数","超限10%除外超限率(%)"])
T_20天与500筛选.rename(columns={'理应在线天数':'应在线天数','超限10%除外数':'超限数','超限10%除外超限率(%)':'超限率'},inplace=True)
T_20天与500筛选=T_20天与500筛选.sort_values(by="超限率",ascending=False,ignore_index=True)
T_20天与500筛选.to_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——在线天数大于等于20天货车数大于500的站点数据.xlsx",sheet_name='在线天数大于等于20天货车数大于500的站点数据')


'''区县超限率排名'''

T_20天与500筛选= pd.read_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——在线天数大于等于20天货车数大于500的站点数据.xlsx")
T_20天与500筛选=pd.DataFrame(T_20天与500筛选,columns=["地市","区县","货车数","超限数"])
区县超限率排序=T_20天与500筛选.groupby([T_20天与500筛选['地市'],T_20天与500筛选['区县']]).sum()
区县超限率排序['超限率']=区县超限率排序.apply(lambda x:  x['超限数']/x['货车数'], axis= 1).round(4)
区县超限率排序['超限率']= 区县超限率排序['超限率'].apply(lambda x: format(x, '.2%'))
区县超限率排序=区县超限率排序.sort_values('超限率',ascending=False)
区县超限率排序.to_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总——区县超限率排序.xlsx",sheet_name='区县超限率排序')

