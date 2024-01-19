from elasticsearch import Elasticsearch
import pandas as pd

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)

#es = Elasticsearch(hosts='192.168.1.42:9100', http_auth=('elastic', 'T1x034bu7Q'),port=9200, timeout=50000)
#es = Elasticsearch([{"host":"192.168.1.42","port":"9100"}])
es = Elasticsearch(hosts=["http://username:password@10.249.2.144:5601/"])
body = {

"query": {
"match_all": {}
},
"_source": {
    "includes": [ 'total_weight',  'area_county',
        'overrun_rate', 'limit_weight', 'record_code',
       'area_city','site_name', 'law_judgment',
       'out_station_time', 'is_truck', 'car_no', 'area_province',
       'out_station','status'
       ]
  },
"size": 1000

}
print('1')
result = es.search(index="t_bas_pass_data_71", body={"query": {"match_all": {}}}, filter_path=['hits.hits._source'])
print(result)
# allDoc = es.search(index='test', body=body)
# print('2')
# items = allDoc['hits']['hits']
#
# print([i['_source'] for i in items])

print('3')
allDoc = es.search(index='t_bas_pass_data_71', body=body)
print('4')
items = allDoc['hits']['hits']

print('item是')
#print(items)

'''用来查看单条数据结构'''
nn=[i['_source'] for i in items]

print('good')
print(nn[0])
# mm=[eval(i['data']) for i in nn]
hh=pd.json_normalize(nn)
print(hh.head(5))
print(hh.columns)
#hh.to_excel(r"F:\报表输出记录\8月\2022-08-24\es测试库的数据_1.xlsx",index=False)
