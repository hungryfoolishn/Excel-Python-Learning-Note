import pymysql
import pandas as pd
import  time

cs = input("请输入城市名：")
df_区县排列= pd.read_excel("C:/Users/Administrator/Desktop/输出报表/{}市月报表/{}市区县模板.xlsx".format(cs,cs))
df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)


df_报修点位统计= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")
df_报修点位统计.columns = df_报修点位统计.iloc[0]
df_报修点位统计= df_报修点位统计.iloc[1:].reset_index(drop=True)

df_数据汇总=df_数据汇总[(df_数据汇总.地市 =='{}'.format(cs))]
df_报修点位统计=df_报修点位统计[df_报修点位统计.地市 =='{}'.format(cs)]


"""附件1"""

货车数=df_数据汇总.groupby(['区县'])['货车数'].sum()
超限数=df_数据汇总.groupby(['区县'])['超限10%除外数'].sum()
df_sheet1=pd.merge(货车数,超限数,how='left',on='区县')
df_sheet1['超限率']=df_sheet1.apply(lambda x: x['超限10%除外数']/x['货车数'], axis=1).round(4)
df_sheet1=df_sheet1.fillna(0,inplace=False)
df_sheet1['超限率']= df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
df_sheet1['超限率排名']=df_sheet1['超限率'].rank(ascending=False, method='first')
df_sheet1=pd.merge(df_区县排列,df_sheet1,on='区县',how='left')

"""附件2"""

超限100=df_数据汇总.groupby(['区县'])['超限100%数'].sum()
附件2=pd.merge(货车数,超限100,how='left',on='区县')
附件2['占比数']=附件2.apply(lambda x: x['超限100%数']/x['货车数']*10000, axis=1).round(2)
附件2=pd.merge(df_区县排列,附件2,on='区县',how='left')


"""附件7"""
在用数=df_数据汇总.groupby(['区县'])['站点名称'].count()
报修数=df_报修点位统计.groupby(['区县'])['报修站点名称'].count()
实际站点数=pd.merge(在用数,报修数,on='区县',how='left')
实际站点数=实际站点数.fillna(0,inplace=False)
实际站点数['站点数']=实际站点数.apply(lambda x: x[0] + x[1], axis=1)
实际站点数.站点数 = 实际站点数.站点数 .astype(int)
实际站点数.报修站点名称 = 实际站点数.报修站点名称 .astype(int)
实际站点数.rename(columns={'站点名称':'在用数','报修站点名称':'报修数'},inplace=True)
T_10筛选=df_数据汇总[(df_数据汇总.实际在线天数 < 10)
               &(df_数据汇总.实际在线天数 > 0)
                                      ]
T_10筛选=T_10筛选.groupby([T_10筛选.区县]).count()
T_10筛选 = T_10筛选.loc[:,['站点名称']]
T_10筛选.columns =['站点名称']
T_500筛选=df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                      &(df_数据汇总.货车数 < 500)) ]
T_500筛选=T_500筛选.groupby([T_500筛选.区县]).count()
T_500筛选 = T_500筛选.loc[:,['站点名称']]
T_500筛选.columns =['站点名称']
T_500筛选.rename(columns={'站点名称':'500辆次'},inplace=True)
T_筛选=pd.merge(T_10筛选,T_500筛选,how='left',on='区县')
T_筛选=T_筛选.fillna(value=0)
T_筛选['在线天数＜10天或货车数＜500辆次']=T_筛选['站点名称']+T_筛选['500辆次']
T_10或500=T_筛选['在线天数＜10天或货车数＜500辆次']
t=df_数据汇总[(df_数据汇总.实际在线天数==0)]
T_数据为0数= t.groupby([t['区县']]).count()
T_数据为0数 = T_数据为0数.loc[:,['实际在线天数']]
T_数据为0数.columns =['实际在线天数']
T_数据为0数.rename(columns={'实际在线天数':'数据为0'},inplace=True)
附件7=pd.merge(df_区县排列,实际站点数,how='left',on='区县')
附件7=pd.merge(附件7,T_数据为0数,how='left',on='区县')
附件7=pd.merge(附件7,T_10或500,how='left',on='区县')
附件7=附件7.fillna(0,inplace=False)
附件7['异常数']=附件7.apply(lambda x: x['报修数'] + x['数据为0']+x['在线天数＜10天或货车数＜500辆次'], axis=1)
附件7['异常数']=附件7['异常数'].astype('float')
附件7['站点数']=附件7['站点数'].astype('float')
附件7['完好率']=附件7['异常数']/附件7['站点数']
附件7=附件7.fillna(0,inplace=False)
附件7['完好率']= 附件7['完好率'].apply(lambda x: format(x, '.2%'))
附件7=pd.DataFrame(附件7,columns=["区县","站点数","报修数","异常数","完好率"])


"""湖州附件1"""
站点数=pd.DataFrame(附件7,columns=["区县","站点数"])
货车数=df_数据汇总.groupby(['区县'])['货车数'].sum()
超限数=df_数据汇总.groupby(['区县'])['超限10%除外数'].sum()
湖州附件1=pd.merge(站点数,货车数,how='left',on='区县')
湖州附件1=pd.merge(湖州附件1,超限数,how='left',on='区县')
湖州附件1['超限率']=df_sheet1.apply(lambda x: x['超限10%除外数']/x['货车数'], axis=1).round(4)
湖州附件1=湖州附件1.fillna(0,inplace=False)
湖州附件1['超限率']=湖州附件1['超限率'].apply(lambda x: format(x, '.2%'))
湖州附件1=pd.merge(df_区县排列,湖州附件1,on='区县',how='left')



"""湖州附件3"""
在用数=df_数据汇总.groupby(['区县'])['站点名称'].count()
报修数=df_报修点位统计.groupby(['区县'])['报修站点名称'].count()
实际站点数=pd.merge(在用数,报修数,on='区县',how='left')
实际站点数=实际站点数.fillna(0,inplace=False)
实际站点数['站点数']=实际站点数.apply(lambda x: x[0] + x[1], axis=1)
实际站点数.站点数 = 实际站点数.站点数 .astype(int)
实际站点数.报修站点名称 = 实际站点数.报修站点名称 .astype(int)
实际站点数.rename(columns={'站点名称':'在用数','报修站点名称':'报修数'},inplace=True)
T_10筛选=df_数据汇总[(df_数据汇总.实际在线天数 < 10)
               &(df_数据汇总.实际在线天数 > 0)
                                      ]
T_10筛选=T_10筛选.groupby([T_10筛选.区县]).count()
T_10筛选 = T_10筛选.loc[:,['站点名称']]
T_10筛选.columns =['站点名称']
T_500筛选=df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                      &(df_数据汇总.货车数 < 500)) ]
T_500筛选=T_500筛选.groupby([T_500筛选.区县]).count()
T_500筛选 = T_500筛选.loc[:,['站点名称']]
T_500筛选.columns =['站点名称']
T_500筛选.rename(columns={'站点名称':'500辆次'},inplace=True)
T_筛选=pd.merge(T_10筛选,T_500筛选,how='left',on='区县')
T_筛选=T_筛选.fillna(value=0)
T_筛选['在线天数＜10天或货车数＜500辆次']=T_筛选['站点名称']+T_筛选['500辆次']
T_10或500=T_筛选['在线天数＜10天或货车数＜500辆次']
t=df_数据汇总[(df_数据汇总.实际在线天数==0)]
T_数据为0数= t.groupby([t['区县']]).count()
T_数据为0数 = T_数据为0数.loc[:,['实际在线天数']]
T_数据为0数.columns =['实际在线天数']
T_数据为0数.rename(columns={'实际在线天数':'数据为0'},inplace=True)
附件7=pd.merge(df_区县排列,实际站点数,how='left',on='区县')
附件7=pd.merge(附件7,T_数据为0数,how='left',on='区县')
附件7=pd.merge(附件7,T_10或500,how='left',on='区县')
附件7=附件7.fillna(0,inplace=False)
附件7['异常数']=附件7.apply(lambda x: x['报修数'] + x['数据为0']+x['在线天数＜10天或货车数＜500辆次'], axis=1)
附件7['异常数']=附件7['异常数'].astype('float')
附件7['站点数']=附件7['站点数'].astype('float')
附件7['完好率']=附件7['异常数']/附件7['站点数']
附件7=附件7.fillna(0,inplace=False)
附件7['完好率']= 附件7['完好率'].apply(lambda x: format(x, '.2%'))
附件7=pd.DataFrame(附件7,columns=["区县","站点数","报修数","异常数","完好率"])
湖州附件3=pd.DataFrame(附件7,columns=["区县","站点数","报修数","异常数","完好率"])
湖州附件3.rename(columns={'站点名称':'接入数（个）','报修数':'报修数（个）','异常数':'异常数（个）','完好率':'完好率（%）'},inplace=True)



"""湖州附件7"""
超限100=df_数据汇总.groupby(['区县'])['超限100%数'].sum()
附件2=pd.merge(货车数,超限100,how='left',on='区县')
附件2['占比数']=附件2.apply(lambda x: x['超限100%数']/x['货车数']*10000, axis=1).round(2)
附件2=pd.merge(df_区县排列,附件2,on='区县',how='left')
湖州附件7=pd.merge(df_区县排列,附件2,on='区县',how='left')




if cs == '杭州':
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/杭州市月报表/杭州月报.xlsx')as writer1:
        df_sheet1.to_excel(writer1, sheet_name='附件1', index=True)
        附件2.to_excel(writer1, sheet_name='附件2', index=True)
        附件7.to_excel(writer1, sheet_name='附件7', index=True)
else:
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/湖州市月报表/湖州月报.xlsx')as writer1:
        湖州附件1.to_excel(writer1, sheet_name='附件1', index=True)
        湖州附件3.to_excel(writer1, sheet_name='附件3', index=True)
        湖州附件3.to_excel(writer1, sheet_name='附件7', index=True)



"""
附件7.apply(lambda x: x['异常数']+['站点数']*100, axis=1)
df_sheet1=pd.DataFrame(df_sheet1,columns=["地市","货车数","超限10%除外数","超限率","超限率排名","实际站点数","接入数（修正后）","实际完好率","调整后完好率"])

df_sheet1['超限率排名'] = df_sheet1['超限率排名'].apply(np.int64)
print(df_sheet1)

df_sheet1.rename(columns={'超限10%除外数':'超限数'},inplace=True)
print(df_sheet1)"""

"""with pd.ExcelWriter("C:/Users/Administrator/Desktop/输出报表/杭州市月报表/附件1.xlsx")as writer1:
  df_sheet1.to_excel(writer1,sheet_name='附件1',index=False)
  test_宁波.to_excel(writer1,sheet_name ='宁波',index=False)"""



"""df_报修点位统计= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")
df_报修点位统计.columns = df_报修点位统计.iloc[0]
df_报修点位统计= df_报修点位统计.iloc[1:].reset_index(drop=True)
print(df_报修点位统计.head())

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


在用数=df_数据汇总.groupby(['地市'])['站点名称'].count()
print(在用数)

报修数=df_报修点位统计.groupby(['地市'])['报修站点名称'].count()
print(报修数)

实际站点数=pd.merge(在用数,报修数,on='地市',how='left')
实际站点数=实际站点数.fillna(0,inplace=False)
实际站点数['实际站点数']=实际站点数.apply(lambda x: x[0] + x[1], axis=1)
实际站点数.实际站点数 = 实际站点数.实际站点数 .astype(int)
实际站点数.报修站点名称 = 实际站点数.报修站点名称 .astype(int)
实际站点数.rename(columns={'站点名称':'在用数','报修站点名称':'实际报修数'},inplace=True)
print(实际站点数)

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

print(T_10或500)

t=df_数据汇总[(df_数据汇总.实际在线天数==0)]

T_数据为0数= t.groupby([t['地市']]).count()
T_数据为0数 = T_数据为0数.loc[:,['实际在线天数']]
T_数据为0数.columns =['实际在线天数']
T_数据为0数.rename(columns={'实际在线天数':'数据为0'},inplace=True)
print(T_数据为0数)

站点设备完好率=pd.merge(df_接入数,实际站点数,on='地市',how='left')
站点设备完好率.loc[11,'在用数'] = T_义乌_在线数
站点设备完好率.loc[11,'实际报修数'] = T_义乌_报修数
站点设备完好率.loc[11,'实际站点数'] = T_义乌_报修数+T_义乌_在线数

print(站点设备完好率)

站点设备完好率=站点设备完好率.fillna(0,inplace=False)
站点设备完好率[['在用数','实际报修数','实际站点数']] = 站点设备完好率[['在用数','实际报修数','实际站点数']].apply(np.int64)
站点设备完好率['报修数']=站点设备完好率.apply(lambda x: x['接入数（修正后）']-x['在用数'], axis= 1)
站点设备完好率.loc[站点设备完好率['报修数'] < 0, '报修数'] = 0
print(站点设备完好率)

站点设备完好率=pd.merge(站点设备完好率,T_数据为0数,on='地市',how='left')

站点设备完好率=pd.merge(站点设备完好率,T_10或500,on='地市',how='left')
print(站点设备完好率)

站点设备完好率.loc[11,'数据为0'] = T_义乌_数据为0数
站点设备完好率.loc[11,'在线天数＜10天或货车数＜500辆次'] = T_义乌_10_500数

站点设备完好率=站点设备完好率.fillna(0,inplace=False)

站点设备完好率[['数据为0','在线天数＜10天或货车数＜500辆次']] = 站点设备完好率[['数据为0','在线天数＜10天或货车数＜500辆次']].apply(np.int64)

站点设备完好率=站点设备完好率.drop([12],axis=0)
站点设备完好率['实际完好数']=站点设备完好率['在用数']-站点设备完好率.数据为0-站点设备完好率['在线天数＜10天或货车数＜500辆次']
站点设备完好率['修正完好数']=站点设备完好率.apply(lambda x: min( x['接入数（修正后）'],x['实际完好数']), axis= 1)
站点设备完好率['实际完好率']=站点设备完好率.apply(lambda x: x['实际完好数']/x['实际站点数'], axis= 1).round(4)
站点设备完好率['调整后完好率']=站点设备完好率.apply(lambda x:  x['修正完好数']/x['接入数（修正后）'], axis= 1).round(4)
站点设备完好率=pd.DataFrame(站点设备完好率,columns=["地市","接入数（修正后）","实际站点数","在用数","实际报修数","报修数","数据为0","在线天数＜10天或货车数＜500辆次","实际完好数","实际完好率","修正完好数","调整后完好率"])
print(站点设备完好率)
站点设备完好率.to_excel("C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总/月报表相关数据汇总.xlsx",sheet_name='站点设备完好率')"""







"""W_案件统计.to_excel("C:/Users/Administrator/Desktop/输出报表\月报表相关数据汇总/2022年处罚案件情况统计.xlsx")"""