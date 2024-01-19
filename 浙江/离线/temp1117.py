# -*- coding:utf-8 -*-
import openpyxl
from openpyxl import Workbook
from openpyxl.chart import (
    Reference,
    Series,
    PieChart,
    BarChart,
    BubbleChart,
    AreaChart,
    AreaChart3D
)

# 绘制饼图

wb = openpyxl.Workbook()
ws = wb.active
ws.title = 'pieChart'
rows = [
    ['Pie', 'Sold'],
    ['Apple', 50],
    ['Cherry', 30],
    ['Pumpkin', 10],
    ['Chocolate', 40]
]
# for循环写入Excel
for row in rows:
    ws.append(row)

# 创建一个饼图对象
pie = PieChart()
# 定义标签和数据范围
labels = Reference(ws, min_col=1, min_row=2, max_row=5)
data = Reference(ws, min_col=2, min_row=2, max_row=5)

# 添加数据和标签
pie.add_data(data)
pie.set_categories(labels)

# 设置饼图标题
pie.title = 'Pies sold by category'

# 设置饼图的位置
ws.add_chart(pie, 'C1')
# 保存工作簿
wb.save('pie.xlsx')