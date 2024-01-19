from datetime import date
from datetime import timedelta
from os import remove
from os import path
from win32com.client import Dispatch
from openpyxl import load_workbook
from itchat import auto_login
from itchat import get_chatrooms
from itchat import search_chatrooms
from itchat import send
from itchat import send_image

#获得昨天的日期，并把名字改成0901这样的格式
def getYesterday():
    today = date.today()
    oneday = timedelta(days = 1)
    yesterday = today - oneday
    date_yes = yesterday.strftime("%m%d")
    return date_yes

#获得日报的标准名称和报表路径
def get_ribao():
    global ribao_name
    ribao_name = r't_bas_over_data_31_81.xlsx'
    global path1
    path1 = path.abspath( r'C:\Users\stayhungary\Desktop')+"\\"

    excel_path = path1 + ribao_name
    print(excel_path)
    # excel_path =  ribao_name
    return excel_path

#把报表中的需要通报的内容导出为图片
def excel_export(excel_range, name):
    rng=ws.Range(excel_range)
    rng.CopyPicture()
    c=ws.ChartObjects().Add(0,0,rng.Width,rng.Height).Chart
    c.Parent.Select()
    c.paste()
    c.Export(path1 + name + '.png', "png")
    c.Parent.Delete()

#获取通报的文字内容
def get_body():
    wb = load_workbook(r'C:\Users\stayhungary\Desktop\t_bas_over_data_31_81.xlsx')
    sheet1 = wb['明细']
    body = sheet1['A2'].value
    return body

def main():
    excel_path = get_ribao()
    xl = Dispatch('Excel.Application')
    xl.Visible = False
    wb = xl.Workbooks.Open(excel_path)
    global ws
    ws = wb.Sheets('明细')
    body = get_body()
    print(body)
    print('日通报内容获取成功')
    # excel_export('A1:R18', '明细')
    # excel_export('A20:R34', '明细')
    print('图片导出成功')
    wb.Saved = True#不保存文件
    wb.Close()
    xl.Quit()
    #通过微信把2张图片发到固定的群里
    auto_login(hotReload=True)
    get_chatrooms() #如果是发到群里的消息或文件，必须保存群到通讯录才能用
    room_store = search_chatrooms(name = "自营厅店长群")[0]['UserName'] #不能发给自己
    room_agent = search_chatrooms(name = "浦东局专营渠道代理商群")[0]['UserName']
    room_qudao = search_chatrooms(name = "浦东实体渠道运营中心")[0]['UserName']
    # room_store = search_chatrooms(name = "周末报表群")[0]['UserName'] #不能发给自己
    # room_agent = search_chatrooms(name = "周末报表群")[0]['UserName']
    # room_qudao = search_chatrooms(name = "周末报表群")[0]['UserName']
    f = path1 + "store.png" #微信附件必须是英文
    g = path1 + "agent.png"
    send_image(f,toUserName=room_store)
    send_image(g,toUserName=room_agent)
    send(body, toUserName=room_qudao)
    print("发送成功")
    remove(f)
    remove(g)
    print("删除文件成功")

if __name__ =="__main__":
    main()

