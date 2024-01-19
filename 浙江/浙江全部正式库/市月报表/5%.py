import pandas as pd
import seaborn as sns
data = pd.read_excel(r"C:\Users\stayhungary\Desktop\test.xlsx")
def highlight_max(s):
 '''
 对列最大值高亮（黄色）处理
 '''
 is_max = s == s.max()
 return ['background-color: yellow' if v else '' for v in is_max]

i=data.style.apply(highlight_max,subset=['货车数', '超限数', '超限10%除外超限率(%)','超限20%除外超限率(%)'])
print(i)
cmap = sns.diverging_palette(10,250,sep=50,as_cmap=True)

data.style.background_gradient(cmap='Blues',subset=['货车数']).to_excel('style.xlsx', engine='openpyxl')