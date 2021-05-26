from pymongo import MongoClient
import csv
import time
from datetime import timedelta
start_time = time.monotonic()
file_1 = open('Odata2019File.csv', 'r')
reader1 = csv.DictReader(file_1, delimiter=';')
file_2 = open('Odata2020File.csv', 'r')
reader2 = csv.DictReader(file_2, delimiter=';')
mongo_client = MongoClient()
db=mongo_client.db
db.zno.drop()
header= ['OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName','ClassProfileNAME',
        'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus',
        'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName',
        'histTest', 'HistLang',	'histTestStatus',	'histBall100', 'histBall12',	'histBall', 'histPTName', 'histPTRegName',
        'histPTAreaName',	'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus',	'mathBall100', 'mathBall12', 'mathBall',
        'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest', 'physLang',	'physTestStatus', 'physBall100',
        'physBall12', 'physBall', 'physPTName', 'physPTRegName', 'physPTAreaName',	'physPTTerName', 'chemTest', 'chemLang',
        'chemTestStatus',	'chemBall100', 'chemBall12', 'chemBall', 'chemPTName',	'chemPTRegName', 'chemPTAreaName', 'chemPTTerName',
        'bioTest', 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName',	'bioPTRegName', 'bioPTAreaName',
        'bioPTTerName', 'geoTest', 'geoLang',	'geoTestStatus', 'geoBall100',	'geoBall12', 'geoBall', 'geoPTName',
        'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus',	'engBall100',	'engBall12',	'engDPALevel',
        'engBall', 'engPTName',	'engPTRegName', 'engPTAreaName', 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100',
        'fraBall12', 'fraDPALevel',	'fraBall', 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName','deuTest',
        'deuTestStatus', 'deuBall100', 'deuBall12',	'deuDPALevel', 'deuBall', 'deuPTName',	'deuPTRegName', 'deuPTAreaName',
        'deuPTTerName', 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName',
        'spaPTRegName', 'spaPTAreaName', 'spaPTTerName', 'Year']
f = open('log.txt', 'w')

for each in reader1:
        row={}
        for field in header:
            row[field]=each[field]
        db.zno.insert_one(row)
for each in reader2:
    row={}
    for field in header:
        row[field]=each[field]
    db.zno.insert_one(row)
end_time = time.monotonic()
print('Запись файлов прошла за ' + str(timedelta(seconds=end_time - start_time)))
# запис у текстовий файл часу завантаження даних
l = [str(timedelta(seconds=end_time - start_time))]
for index in l:
    f.write('Запись файлов прошла за ' + index + '\n')
f.close()

query = db.zno.aggregate([
        {"$match": {"histTestStatus": "Зараховано"}},
        {"$group": {
            "_id": {
                "regname": "$REGNAME",
                "zno_year": "$zno_year",
                "histteststatus": "$histTestStatus"
            },
            "avgball": {
                "$avg": "$histBall100"
            }
        }},
        {"$sort": {"_id": 1}}
    ])

with open('results.csv', 'w', encoding="utf-8") as result_csvfile:
    csv_writer = csv.writer(result_csvfile)
    csv_writer.writerow(['region', 'year', 'Average of history'])
    for k in query:
        row = [k["_id"]["region"],str(k["_id"]["year"]), "Avg"]
        csv_writer.writerow(row)
    print(f'SELECTED: Average  of history')