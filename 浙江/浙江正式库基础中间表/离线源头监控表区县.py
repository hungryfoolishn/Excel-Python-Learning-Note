from tkinter import *
from tkinter import filedialog

import pandas as pd

#表头处理函数
def set_columns_date_frame(df,select_rows,tail_rows):
    #得到一个包含列名的大的数据块
    ff=df.values
    #数据块分为两块：不规则列名块和规则数据块
    #第一步处理列名
    if tail_rows==0:
        f1 = pd.DataFrame(ff[0:select_rows, :]).fillna(method='ffill', axis=0)
        #第二步处理后列名和数据块打包
        f2 = pd.DataFrame(ff[select_rows:, :], columns=f1.values[-1])
    else:
        f1 = pd.DataFrame(ff[0:select_rows, :]).fillna(method='ffill', axis=0)
        # 第二步处理后列名和数据块打包
        f2 = pd.DataFrame(ff[select_rows:-tail_rows, :], columns=f1.values[-1])
    return f2

def qq():
    pass

def func1():
    #a=filedialog.asksaveasfilename()#返回文件名

    #a =filedialog.asksaveasfile()#会创建文件

    #a =filedialog.askopenfilename()#返回文件名

    #a =filedialog.askopenfile()#返回文件流对象

    #a =filedialog.askdirectory()#返回目录名

    a =filedialog.askopenfilenames()#可以返回多个文件名

    #a=filedialog.askopenfiles()#多个文件流对象
def sr_1():
    global a1
    a1 = filedialog.askopenfilenames()  # 可以返回多个文件名的地址的元组
    sr_e_1.set(a1)
def sr_2():
    global a2
    a2 = filedialog.askopenfilename()  # 可以返回多个文件名的地址的元组
    sr_e_2.set(a2)
def sr_3():
    sr_e_5.set("")
    c_e_1.set("")
    global a3
    a3 = filedialog.askopenfilename()  # 可以返回多个文件名的地址的元组
    sr_e_3.set(a3)
def sr_4():
    global a4
    a4 = filedialog.askopenfilename()  # 可以返回多个文件名的地址的元组
    sr_e_4.set(a4)
def sr_5():
    sr_e_3.set("")
    c_e_1.set("")
    global a5
    a5 = filedialog.askopenfilename()  # 可以返回多个文件名的地址的元组
    sr_e_5.set(a5)
def tjfx():
    EE.config(state='readonly')
    # try:

    cs = sr_e_1.get()


    df_数据汇总 = pd.read_excel(a2)
    # df_数据汇总 = set_columns_date_frame(df_数据汇总, 0, 0)

    df_接入数 = pd.read_excel(a3)
    df_报修点位统计 = pd.read_excel(a4)

    if cs == '浙江':
        """站点完好率"""

        df_报修点位统计.columns = df_报修点位统计.iloc[0]
        df_报修点位统计 = df_报修点位统计.iloc[1:].reset_index(drop=True)
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
        df_数据汇总 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        在用数 = df_数据汇总.groupby(['地市'])['站点名称'].count()
        df_报修点位统计 = df_报修点位统计[df_报修点位统计.区县 != '义乌']
        报修数 = df_报修点位统计.groupby(['地市'])['报修站点名称'].count()
        实际站点数 = pd.merge(在用数, 报修数, on='地市', how='outer')
        实际站点数 = 实际站点数.fillna(0, inplace=False)
        实际站点数['实际站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
        实际站点数.实际站点数 = 实际站点数.实际站点数.astype(int)
        实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
        实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '实际报修数'}, inplace=True)
        T_10筛选 = df_数据汇总[(df_数据汇总.实际在线天数 < 10)
                         & (df_数据汇总.实际在线天数 > 0)
                         ]
        T_10筛选 = T_10筛选.groupby([T_10筛选.地市]).count()
        T_10筛选 = T_10筛选.loc[:, ['站点名称']]
        T_10筛选.columns = ['站点名称']
        T_500筛选 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                           & (df_数据汇总.货车数 < 500))]
        T_500筛选 = T_500筛选.groupby([T_500筛选.地市]).count()
        T_500筛选 = T_500筛选.loc[:, ['站点名称']]
        T_500筛选.columns = ['站点名称']
        T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
        T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on='地市')
        T_筛选 = T_筛选.fillna(value=0)
        T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
        T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
        t = df_数据汇总[(df_数据汇总.实际在线天数 == 0)]
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


        """做sheet1"""

        df_数据汇总 = pd.read_excel(a2)
        # df_数据汇总 = set_columns_date_frame(df_数据汇总, 0, 0)

        df_数据汇总 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                               & (df_数据汇总.货车数 > 500))]
        T_义乌 = df_数据汇总[(df_数据汇总['区县'] == '义乌')]
        T_义乌_货车数 = T_义乌.groupby(['区县'])['货车数'].sum()
        T_义乌_超限数 = T_义乌.groupby(['区县'])['超限20%除外数'].sum()
        df_数据汇总 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        货车数 = df_数据汇总.groupby(['地市'])['货车数'].sum()
        超限数 = df_数据汇总.groupby(['地市'])['超限20%除外数'].sum()
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
        df_数据汇总 = pd.read_excel(a2)
        # df_数据汇总 = set_columns_date_frame(df_数据汇总, 0, 0)
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
        #print(区县超限率排序)
        """储存表"""
        #q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
        with pd.ExcelWriter('{}/月相关汇总表.xlsx'.format(b1)) as writer1:
            df_sheet1.to_excel(writer1, sheet_name='sheet1', index=True)
            站点设备完好率.to_excel(writer1, sheet_name='站点设备完好率', index=True)
            区县超限率排序.to_excel(writer1, sheet_name='区县超限率排序', index=True)
            T_20天与500筛选1.to_excel(writer1, sheet_name='在线天数大于等于20天货车数大于500的站点数据', index=True)


        c_e_1.set("处理已完成，请查看！！！")
    # except:
    #     c_e_1.set("出现了某些错误")

def sc_10():
    a = filedialog.asksaveasfile()  # 会创建文件 适用于单个文件的输出
def sc_1():
    c_e_1.set("")
    global b1
    b1 = filedialog.askdirectory()  # 返回目录名 适用于多个文件的输出
    sc_e_1.set(b1)
def czhs():
    EE.config(state='normal')
    sr_e_1.set("")
    sr_e_5.set("")
    sr_e_3.set("")
    c_e_1.set("")



root=Tk()
root.resizable(width=False, height=False)
s1=StringVar()
s1.set("")
s2=StringVar()
s2.set("")
root.title('月度相关汇总表')





k3=LabelFrame(root,text="输入需要分析的参数 ^-^",width=500,height=200)
k3.grid(row=1,column=0,rowspan=2)


sr_e_1=StringVar()
sr_e_1.set("")
Label(k3,text='请输入省会或城市',width=15).grid(row=0,column=0,rowspan=1,pady=2,padx=4)
EE=Entry(k3,textvariable=sr_e_1,width=40)
EE.grid(row=0,column=1,rowspan=1,pady=2,padx=4)
#Button(k3,text="选择文件",width=15,command=sr_1).grid(row=0,column=2,rowspan=1,pady=2,padx=4)
Button(k3,text="重-置",width=15,command=czhs).grid(row=0,column=2,rowspan=1,pady=2,padx=4)

sr_e_2=StringVar()
sr_e_2.set("")
Label(k3,text='选择月度汇总表',width=15).grid(row=1,column=0,rowspan=1,pady=2,padx=4)
Entry(k3,textvariable=sr_e_2,width=40,state='readonly').grid(row=1,column=1,rowspan=1,pady=2,padx=4)
Button(k3,text="选择文件",width=15,command=sr_2).grid(row=1,column=2,rowspan=1,pady=2,padx=4)

sr_e_4=StringVar()
sr_e_4.set("")
Label(k3,text='选择报修数文件',width=15).grid(row=2,column=0,rowspan=1,pady=2,padx=4)
Entry(k3,textvariable=sr_e_4,width=40,state='readonly').grid(row=2,column=1,rowspan=1,pady=2,padx=4)
Button(k3,text="选择文件",width=15,command=sr_4).grid(row=2,column=2,rowspan=1,pady=2,padx=4)

Frame(root,width=550,height=5,bg='red').grid(row=4,column=0,rowspan=1,pady=2,padx=4)


sjbb=LabelFrame(root,text="省级",width=500,height=200)
sjbb.grid(row=5,column=0,rowspan=2)

sr_e_3=StringVar()
sr_e_3.set("")
Label(sjbb,text='选择接入数文件',width=15).grid(row=3,column=0,rowspan=1,pady=2,padx=4)
Entry(sjbb,textvariable=sr_e_3,width=40,state='readonly').grid(row=3,column=1,rowspan=1,pady=2,padx=4)
Button(sjbb,text="选择文件",width=15,command=sr_3).grid(row=3,column=2,rowspan=1,pady=2,padx=4)

Frame(root,width=300,height=5,bg='red').grid(row=9,column=0,rowspan=1,pady=2,padx=4)

qsbb=LabelFrame(root,text="市级",width=500,height=200)
qsbb.grid(row=10,column=0,rowspan=2)

sr_e_5=StringVar()
sr_e_5.set("")
Label(qsbb,text='选择市区模版',width=15).grid(row=4,column=0,rowspan=1,pady=2,padx=4)
Entry(qsbb,textvariable=sr_e_5,width=40,state='readonly').grid(row=4,column=1,rowspan=1,pady=2,padx=4)
Button(qsbb,text="选择文件",width=15,command=sr_5).grid(row=4,column=2,rowspan=1,pady=2,padx=4)

Frame(root,width=550,height=5,bg='red').grid(row=13,column=0,rowspan=1,pady=2,padx=4)


k6=LabelFrame(root,text="为输出找个目录吧！",width=500,height=200)
k6.grid(row=25,column=0,rowspan=2)

sc_e_1=StringVar()
sc_e_1.set("")
Label(k6,text='输出到',width=15).grid(row=0,column=0,rowspan=1,pady=2,padx=4)
Entry(k6,textvariable=sc_e_1,width=40,state='readonly').grid(row=0,column=1,rowspan=1,pady=2,padx=4)
Button(k6,text="选择目录",width=15,command=sc_1).grid(row=0,column=2,rowspan=1,pady=2,padx=4)
# Entry(k6,textvariable=sc_dwj_e_1,width=40).grid(row=1,column=1,rowspan=1,pady=2,padx=4)
# Button(k6,text="处理情况",width=15,command=sc_dwj).grid(row=1,column=0,rowspan=1,pady=2,padx=4)
# Button(k6,text="保存结果",width=15,command=sc_dwj).grid(row=1,column=2,rowspan=1,pady=2,padx=4)

Frame(root,width=550,height=5,bg='red').grid(row=28,column=0,rowspan=1,pady=2,padx=4)


k8=LabelFrame(root,text="进行分析并获取结果",width=500,height=200)
k8.grid(row=30,column=0,rowspan=2)

c_e_1=StringVar()
c_e_1.set("")
Label(k8,text='处理结果',width=15).grid(row=0,column=0,rowspan=1,pady=2,padx=4)
Entry(k8,textvariable=c_e_1,width=40,state='readonly').grid(row=0,column=1,rowspan=1,pady=2,padx=4)
Button(k8,text="提交分析",width=15,command=tjfx).grid(row=0,column=2,rowspan=1,pady=2,padx=4)

#Text(k8,width=40,height=10).grid(row=1,column=0,rowspan=1,columnspan=1,pady=2,padx=4)
#Entry(k8,textvariable=sc_dwj_e_1,width=40).grid(row=1,column=1,rowspan=1,pady=2,padx=4)
#Button(k8,text="结果预览",width=15).grid(row=1,column=1,rowspan=1,pady=2,padx=4)
#Button(k8,text="保存结果",width=15,command=sc_dwj).grid(row=1,column=2,rowspan=1,pady=2,padx=4)

root.mainloop()