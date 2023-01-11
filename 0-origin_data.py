import requests, pymongo, json

url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/hotel_C_f.json'
res = requests.get(url)
res.encoding = 'utf-8-sig'

data = res.json()['XML_Head']['Infos']['Info']

client = pymongo.MongoClient('mongodb+srv://hsinyi:10656025@cluster0.f3x7ztv.mongodb.net/test')

db = client['guesthouse_test']
col = db['guesthouse_origin']

col.drop()
col.insert_many(data)
print('匯入民宿資料成功')

# ------ population -------

url = 'https://od.moi.gov.tw/api/v1/rest/datastore/301000000A-000605-067'
res = requests.get(url)
res.encoding = 'utf-8-sig'

data = res.json()['result']['records'][1:-6]
col = db['tw_population_origin']

col.drop()
col.insert_many(data)
print('匯入人口地區資料成功')