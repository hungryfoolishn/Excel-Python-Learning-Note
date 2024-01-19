import  os
import win32gui, win32api, win32con, win32com
from win32com.client import Dispatch
from PIL import ImageGrab,Image
from time import sleep
import pyperclip

# 调用win32api的模拟点击功能实现ctrl+v粘贴快捷键
def ctrlV():
    win32api.keybd_event(17,0,0,0)  #ctrl键位码是17
    win32api.keybd_event(86,0,0,0)  #v键位码是86
    win32api.keybd_event(86,0,win32con.KEYEVENTF_KEYUP,0) #释放按键
    win32api.keybd_event(17,0,win32con.KEYEVENTF_KEYUP,0)

# 调用win32api的模拟点击功能实现alt+s微信发送快捷键 （可以根据自己微信发送快捷键是什么来进行调整）
def altS():
    win32api.keybd_event(18, 0, 0, 0)    #Alt
    win32api.keybd_event(83,0,0,0) #s
    win32api.keybd_event(83,0,win32con.KEYEVENTF_KEYUP,0) #释放按键
    win32api.keybd_event(18,0,win32con.KEYEVENTF_KEYUP,0)

# 调用win32gui调用桌面窗口，获取指定窗口句柄id,激活窗口  ，向函数传递窗口名称to_weixin
def wx_send(to_weixin):
    for i in range(0,len(to_weixin)):
        hw = win32gui.FindWindow(None, to_weixin[i])  # 获取窗口句柄
        win32gui.GetClassName(hw)  # 获取窗口classname
        title = win32gui.GetWindowText(hw)  # 获取窗口标题
        win32gui.GetDlgCtrlID(hw)
        win32gui.SetForegroundWindow(hw) # 激活窗口
        sleep(1)
        ctrlV()
        sleep(1)
        altS()

# 使win32调用excel,刷新数据，并发送微信，refreshall刷新excel的所有数据来源，我的数据来源是pq搭建的链接数据库的模型，也可以搭载爬虫的数据，这样就可以完成自动刷新数据并发送微信的操作，解放双手更多的时间来学习
def wkb_Operate(class_picture,wkb_path,sleep_time):
        os.system('taskkill /IM EXCEL.exe /F')
        xlapp = win32com.client.gencache.EnsureDispatch('Excel.Application')
        xlapp.Visible = 1
        xlapp.DisplayAlerts = False # 关闭警告
        wkb = xlapp.Workbooks.Open(wkb_path)
        wkb.RefreshAll()
        sleep(sleep_time)
        print('文件【{}】已打开！'.format(wkb_path))
        try:
            for key,vlaue in class_picture.items():

                to_weixin = class_picture[key]['发送群']
                to_sontent = class_picture[key]['发送文本']
                sheet_name = class_picture[key]['sheetname']
                range_pic = class_picture[key]['图片区域']

                pyperclip.copy(to_sontent)
                wx_send(to_weixin)

                sheet_msg = wkb.Worksheets(sheet_name)
                sheet_msg.Range(range_pic).CopyPicture()
                wkb.Worksheets.Add().Name = 'picture'
                sheet_picture = wkb.Worksheets('picture')
                sleep(1)
                sheet_picture.Range('A1').Select()
                sheet_picture.Paste()
                sleep(1)
                xlapp.Selection.ShapeRange.Name = 'pic_name'
                sheet_picture.Shapes('pic_name').Copy()
                sleep(1)
                img = ImageGrab.grabclipboard()
                sleep(1)
                wx_send(to_weixin)
                wkb.Worksheets('picture').Delete()
                print('#粘贴 成功:%s',sheet_name)
        except BaseException as e:
            print(e)
            pass
        wkb.Save()
        wkb.Close(1)
        xlapp.Quit()
        print('#更新 成功:%s' % wkb_path)
        pass
# *********************主程序，大致方向是，1、调用wkb_Operate刷新函数刷新exce数据，并返回文本数据播报内容，
#2、调用wx_send激活微信窗口，3、ctrlV()，altS()模拟粘贴发送功能，4、调用excel_picture函数，截图到剪切板上，并调用wx_send（），ctrlV()，altS()微信发送图片
# 微信发送窗口，必须保持一致，按照此名字识别，否则发送不会成功****（抄袭可耻！！作者：故笺）**

# 文件路径：
path_process = r'C:\Users\stayhungary\Desktop\t_bas_over_data_31_80.xlsx'  # 文件夹路径

#*********主程序***************#
class_picture1 = {'pic1':{'发送群':['管理团队','数据中心'],
                          'sheetname':'日监控',
                          '图片区域':'a1:Al50',
                          '发送文本':'截止到目前的流水和PK情况'}}
wkb_Operate(class_picture1,path_process,8)