import requests, re

# 取得open data
url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/hotel_C_f.json'
res = requests.get(url)
res.encoding = 'utf-8-sig'

# 轉成python dict
data = res.json()['XML_Head']['Infos']['Info']

# 處理格式
for d in data:
    # 停車資訊
    parking_info = re.split(':|、',d['Parkinginfo'])
    d['Car'] = {
        'SmallDecker':int(parking_info[1][3:-1]),
        'Scooter': int(parking_info[2][2:-1]),
        'BigDecker': int(parking_info[3][3:-1])
    }

    # 服務項目
    d['Service'] = [content.strip() for content in d['Serviceinfo'].split(',') if content != '']
    del d['Serviceinfo']

    # 圖片處理
    pic = [
        d['Picture1'] if d['Picture1'] != '' else None,
        d['Picture2'] if d['Picture2'] != '' else None,
        d['Picture3'] if d['Picture3'] != '' else None,
    ]
    pic_des = [
        d['Picdescribe1'] if d['Picdescribe1'] != '' else None,
        d['Picdescribe2'] if d['Picdescribe2'] != '' else None,
        d['Picdescribe3'] if d['Picdescribe3'] != '' else None,
    ]

    pic_res = []
    for i in range(len(pic)):
        if pic[i] is not None:
            pic_res.append({
                'Link': pic[i],
                'Describe': pic_des[i]
            })

    d['Picture'] = pic_res

    del d['Picture1']
    del d['Picture2']
    del d['Picture3']

    del d['Picdescribe1']
    del d['Picdescribe2']
    del d['Picdescribe3']


# -------- 匯入 --------
import pymongo
client = pymongo.MongoClient('mongodb+srv://hsinyi:10656025@cluster0.f3x7ztv.mongodb.net/test')

print('正在建立db及collection...')
db = client['guesthouse_test']
col = db['guesthouse']

print('匯入民宿資料...')
col.drop()
col.insert_many(data)
print('匯入完畢')


# 移除多於欄位
result = col.update_many( { }, { '$unset': { 'Grade': '' , 'Map': '', 'Spec':''} } )

# 更新內容為空字串為None
for key in col.find_one().keys():
    col.update_many({ key : '' }, { '$set': { key: None } })

# 資料型態處理
data = col.find()
for d in data:
    taiwan_host = d['TaiwanHost'] if d['TaiwanHost'] is not None else 0
    col.update_one(
                   {'_id' : d['_id'] }, 
                   {
                        '$set' : {
                        'Class' :int(d['Class']), 
                        'TaiwanHost': bool(taiwan_host)
                        }
                    }
                )

# 變更字元，避免轉為html有誤
result = col.update_many( 
    {'Name':{'$regex':'`'}}, 
    {'$set' : {'Name' : d['Name'].replace('`','\'')}} 
)
print(result.matched_count, result.modified_count)


# -----------population-------------

# 取得open data
url = 'https://od.moi.gov.tw/api/v1/rest/datastore/301000000A-000605-067'
res = requests.get(url)
res.encoding = 'utf-8-sig'

# 轉成python dict
data = res.json()['result']['records'][1:-6]

for d in data:
    d['region'] = d['site_id'][:3]
    d['town'] = d['site_id'][3:]
    del d['site_id']

col = db['tw_population']

print('匯入人口地區資料...')
col.drop()
col.insert_many(data)
print('匯入完畢')

# 資料型態處理
data = col.find()
for d in data:
    col.update_one(
                    {'_id' : d['_id'] }, 
                    {
                        '$set' : {
                            'statistic_yyy' :int(d['statistic_yyy']), 
                            'people_total': int(d['people_total']), 
                            'area': float(d['area']), 
                            'population_density': int(d['population_density'])
                            }
                        }
                    )

