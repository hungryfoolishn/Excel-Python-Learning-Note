
import pandas as pd
import numpy as np
import datetime as dt
import io

df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=False)



df_报修点位统计表= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")
df_报修点位统计表.columns = df_报修点位统计表.iloc[0]
df_报修点位统计表= df_报修点位统计表.iloc[1:].reset_index(drop=False)


I = ["杭州","宁波","温州","嘉兴","湖州","绍兴","金华","衢州","舟山","台州","丽水"]

test1=df_数据汇总.loc[(df_数据汇总.地市=="杭州")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="杭州")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_杭州=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="宁波")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="宁波")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_宁波=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="温州")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="温州")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_温州=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="嘉兴")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="嘉兴")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_嘉兴=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="湖州")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="湖州")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_湖州=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="绍兴")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="绍兴")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_绍兴=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="金华")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="金华")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_金华=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="衢州")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="衢州")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_衢州=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="舟山")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="舟山")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_舟山=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="台州")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="台州")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_台州=pd.concat((test1, test2),axis=1,ignore_index=False)

test1=df_数据汇总.loc[(df_数据汇总.地市=="丽水")]
test2=df_报修点位统计表.loc[(df_报修点位统计表.地市=="丽水")]
test2=test2.drop(['备注'], axis=1)
test2=pd.DataFrame(test2,columns=["报修站点名称","地市","区县"])
test1=pd.DataFrame(test1,columns=["站点名称","地市","区县","理应在线天数","实际在线天数","在线率","货车数","超限数","超限10%除外数","超限10%除外超限率(%)","超限20%除外数","超限20%除外超限率(%)","百吨王数","超限100%数","超限率(%)"])
test_丽水=pd.concat((test1, test2),axis=1,ignore_index=False)

with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/XX月份站点超限明细/站点超限明细.xlsx')as writer1:
  test_杭州.to_excel(writer1,sheet_name ='杭州',index=False)
  test_宁波.to_excel(writer1,sheet_name ='宁波',index=False)
  test_温州.to_excel(writer1, sheet_name='温州', index=False)
  test_嘉兴.to_excel(writer1, sheet_name='嘉兴', index=False)
  test_湖州.to_excel(writer1, sheet_name='湖州', index=False)
  test_绍兴.to_excel(writer1, sheet_name='绍兴', index=False)
  test_金华.to_excel(writer1, sheet_name='金华', index=False)
  test_衢州.to_excel(writer1, sheet_name='衢州', index=False)
  test_舟山.to_excel(writer1, sheet_name='舟山', index=False)
  test_台州.to_excel(writer1, sheet_name='台州', index=False)
  test_丽水.to_excel(writer1, sheet_name='丽水', index=False)



"""test.to_excel("C:/Users/Administrator/Desktop/输出报表/XX月份站点超限明细/站点超限明细——"杭州.xlsx",sheet_name="杭州")"""


