# -*- coding: utf-8 -*-

import pymongo
from pymongo import MongoClient
import datetime

mongo_db_Conn = pymongo.MongoClient('192.168.60.65', 10010).baidu_gg
db_tbl = mongo_db_Conn.content_tbl
ret = db_tbl.find({'status' : 3}, {'url' : 1})

db_5118 = mongo_db_Conn.keywords_5118
insert_index = 0
for con in ret:
    # if not con: continue
    try:
        db_5118.save({'url': con['url'], 'keywords': '', 'status': 0, 'insert_time': datetime.datetime.now()})
        insert_index += 1
    except Exception, e:
        print str(e)
    else:
        continue
print insert_index
