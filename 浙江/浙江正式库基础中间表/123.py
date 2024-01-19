from datetime import datetime

starttime = '2023-04-01'
endtime = '2023-05-01'
starttime = datetime.strptime(starttime, '%Y-%m-%d')
endtime = datetime.strptime(endtime, '%Y-%m-%d')

理应在线天数 = (endtime - starttime).days


from datetime import datetime

starttime = '2023-04-01'
endtime = '2023-05-01'

starttime = datetime.strptime(starttime, '%Y-%m-%d')
endtime = datetime.strptime(endtime, '%Y-%m-%d')

i = (endtime-starttime).days

print(i,理应在线天数)