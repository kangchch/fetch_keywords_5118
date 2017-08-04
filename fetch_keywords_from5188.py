# -*- coding: utf-8 -*-

import requests
import pymongo
import datetime
from lxml import etree
import pdb
import os
import logging

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

sys.path.append('/tools/python_common')
from common_func import logInit

start_url = 'http://www.5118.com/seo/baidurank/'
# 获取url对应的网页源码
def get_source(urls):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1;WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        response = requests.get(start_url + urls, headers=headers)
        if response.status_code != 200:
            return
        response.raise_for_status()
    except requests.RequestException as e:
        print e
        logging.error('response.status_code = %d!' % (response.status_code))
    else:
        return response.text


def insert_Mongodb(mongoConn, url, keywords):
    db = mongoConn.baidu_gg
    collection = db.keywords_5118
    collection.insert({'url': url, 'keywords': keywords})


def geturl_Mongodb(mongodbConn):
    result = []
    if not mongodbConn:
        return None
    db = mongodbConn.baidu_gg
    collection = db.keywords_5118
    ret = collection.find({'status': 0}, {'url': 1})
    [result.append(con['url']) for con in ret if con['url']]
    return result


def update_Mongodb(mongodbConn, url, keywords, status):
    db = mongodbConn.baidu_gg
    collection = db.keywords_5118
    collection.update(
            {'url': url},
            {'$set':
                {
                    'keywords': keywords,
                    'status': status,
                    'update_time': datetime.datetime.now()
                }
            }
                    )

if __name__ == '__main__':

    DIR_PATH = os.path.split(os.path.realpath(__file__))[0]
    LOG_FILE = DIR_PATH + '/logs/' + __file__.replace('.py', '.log')
    logInit(logging.INFO, LOG_FILE, 0, True)

    mongodbConn = pymongo.MongoClient('192.168.60.65', 10010)
    urls = geturl_Mongodb(mongodbConn)
    for url in urls:
        response= get_source(url)
        if not response:
            logging.error('fetch failed! failed url=%s ' %  (url))
            update_Mongodb(mongodbConn, url, '', 2)
        else:
            selector = etree.HTML(response)
            keywords = selector.xpath('//dd[@class="col3-9 word"]/span/a/@title')
            keywords = keywords if keywords else []
            update_Mongodb(mongodbConn, url, keywords, 1)
            logging.info('update_Mongodb url=%s, status=1,update_time=%s' % (url, datetime.datetime.now()))
