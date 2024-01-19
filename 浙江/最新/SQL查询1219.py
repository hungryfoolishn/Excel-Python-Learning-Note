# coding: utf-8
from tkinter import *
from tkinter import filedialog
import pandas as pd
import json
import requests
import base64

# 获取数据
def get_df_from_db(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "query",
        "tableName": sql['tableName'],
        "where": (base64.b64encode(sql['where'].encode())).decode(),
        "columns": sql['columns'],
        "isEncry": "1"
    }
    url = 'https://yhxc.jtyst.zj.gov.cn:7443/zc-interface/db/excuteSql'
    headers = {'content-type': 'application/json'}
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}

    res = requests.post(url, json=data, headers=headers)
    res = json.loads(res.text)
    data = res['data']
    data=pd.DataFrame(data)
    return  data


def tjfx():
    select = sc_dwj_e_2.get()
    lfrom = sc_dwj_e_3.get()
    where = sc_dwj_e_4.get()
    sc_dz = b1
    sql_station = {
        "tableName": "{} ".format(lfrom),
        "where": " {}".format(where),
        "columns": "{}".format(select)
    }
    data1 = get_df_from_db(sql_station)

    with pd.ExcelWriter(r'{}\临时查询.xlsx'.format(b1)) as writer1:
        data1.to_excel(writer1, sheet_name='临时查询', index=True)

def show_data_1(*args):
    sc_dwj_e_1.set("")
    sc_dwj_e_0.set("等待点击进行分析")


def dbhs():
    pass

def sc_1():
    sc_dwj_e_1.set("")
    global b1
    b1 = filedialog.askdirectory()  # 返回目录名 适用于多个文件的输出
    sc_e_1.set(b1)
root=Tk()
s1=StringVar()
s1.set("")
s2=StringVar()
s2.set("")
root.title('临时数据查询')
# kk=Frame(root,width=500,height=200)
# kk.grid(row=0,column=0,rowspan=1)


k5=LabelFrame(root,text="输入确认",width=500,height=200,bg='Red')
k5.grid(row=20,column=0,rowspan=2)
Label(k5,text='select',width=15).grid(row=0,column=0,rowspan=1,pady=2,padx=4)
sc_dwj_e_2=StringVar()
sc_dwj_e_2.set("")
Entry(k5,textvariable=sc_dwj_e_2,width=85).grid(row=0,column=1,columnspan=3,pady=2,padx=4)

Label(k5,text='from',width=15).grid(row=20,column=0,rowspan=1,pady=2,padx=4)
sc_dwj_e_3=StringVar()
sc_dwj_e_3.set("")
Entry(k5,textvariable=sc_dwj_e_3,width=85).grid(row=20,column=1,columnspan=3,pady=2,padx=4)

Label(k5,text='where',width=15).grid(row=40,column=0,rowspan=1,pady=2,padx=4)
sc_dwj_e_4=StringVar()
sc_dwj_e_4.set("")
Entry(k5,textvariable=sc_dwj_e_4,width=85).grid(row=40,column=1,columnspan=3,pady=2,padx=4)



k7=LabelFrame(root,text="输出保存",width=500,height=200,bg='Red')
k7.grid(row=30,column=0,rowspan=2)
sc_e_1=StringVar()
sc_e_1.set("")
Label(k7,text='文件地址',width=15).grid(row=0,column=0,rowspan=1,pady=2,padx=4)
Entry(k7,textvariable=sc_e_1,state='readonly',width=40).grid(row=0,column=1,rowspan=1,pady=2,padx=4)
Button(k7,text="结果保存",width=15,command=sc_1).grid(row=0,column=2,rowspan=1,pady=2,padx=4)


k8=LabelFrame(root,text="分析模块",width=500,height=200,bg='Red')
k8.grid(row=35,column=0,rowspan=2)

sc_dwj_e_0=StringVar()
sc_dwj_e_0.set("")
sc_dwj_e_1=StringVar()
sc_dwj_e_1.set("")
Label(k8,text='分析结果',width=15).grid(row=2,column=0,rowspan=1,pady=2,padx=4)
Entry(k8,textvariable=sc_dwj_e_1,width=40,state='readonly').grid(row=2,column=1,rowspan=1,pady=2,padx=4)
Button(k8,text="提交分析",width=15,command=tjfx).grid(row=2,column=2,rowspan=1,pady=2,padx=4)


root.mainloop()

