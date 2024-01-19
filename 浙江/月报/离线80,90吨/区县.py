import pymysql
import pandas as pd
import numpy as np
import  time


t_bas_pass_data_71 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\企业_源头_站点表9月.xlsx")
t_code_area = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\9月统计\货运源头数据\t_code_area.xls")
U_源头_区域表 = pd.merge(t_bas_pass_data_71 ,t_code_area,left_on='区县',right_on='county_code',how='left')

U_源头_区域表 .to_excel("G:/智诚/日常给出数据汇总/月通报/9月统计/企业_源头_站点表3.xlsx")

