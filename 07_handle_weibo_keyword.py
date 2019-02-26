#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于处理 Web Scraper 采集的提及了品牌关键词的微博数据
# 每个品牌一个 csv 文件，存放在 source 文件夹内
# 以 mentioning 字段记录提及的品牌

# 需要 pip 安装的外部库
from pymongo import MongoClient
from bson.objectid import ObjectId

# 内置库
import os
import re
import csv
import json
import re
import time
import datetime

# 数据库配置
client = MongoClient()
db = client.newrank
keyword_collection = db.sharecar_weibo_keyword

# 其他配置
today = str(datetime.date.today())

def filter_keyword_file(item):
    r = re.search(r'weibo_keyword.*\.csv', item)
    return True if r else False

def assemble_doc(doc):
    m1 = re.match(r'https?:\/\/weibo\.com\/(\d+)\/(\w+)', doc['link-href'])
    m2 = re.match(r'(\d+)月(\d+)日\s(\d+):(\d+)', doc['link'])
    m3 = re.match(r'(\d+)', doc['quote_count'])
    m4 = re.match(r'(\d+)', doc['comment_count'])
    m5 = re.match(r'(\d+)', doc['attitude_count'])
    content = doc['full_text'] if doc['full_text'] != 'null' and doc['time'] == doc['link'] else doc['text']
    return {
        'bid': m1.group(2) if m1 else None,
        'user_id': m1.group(1) if m1 else None,
        'name': doc['name'],
        'content': content,
        'publish_date': m2.group(1) + '-' + m2.group(2) if m2 else None,
        'publish_time': m2.group(3) + ':' + m2.group(4) if m2 else None,
        'quote_count': m3.group(1) if m3 else 0,
        'comment_count': m4.group(1) if m4 else 0,
        'attitude_count': m5.group(1) if m5 else 0,
        'page': doc['web-scraper-start-url']
    }

# 读取 source 文件夹，取出以 weibo_keyword 开头的 csv 文件
keyword_files = filter(filter_keyword_file, os.listdir('source'))

# csv 转 json，并组装成 {bid, user_id, content, publish_at, quote_count, comment_count, attitude_count } 结构
for keyword_file in keyword_files:
    brand = re.match(r'weibo_keyword_(\w+)\.csv', keyword_file).group(1)
    with open('source/' + keyword_file, 'r') as f:
        reader = csv.DictReader(f)
        docs = map(assemble_doc, reader)
        
        for doc in docs:
            _doc = keyword_collection.find_one({'bid': doc['bid']})

            # 已有的微博检查 mentioning 字段是否包含当前品牌
            if _doc:
                if brand not in _doc['mentioning']:
                    _doc['mentioning'].append(brand)
                    keyword_collection.update({
                        '_id': ObjectId(_doc['_id'])}, {
                            '$set': {'mentioning': _doc['mentioning']}})
            else:
                doc['mentioning'] = [brand]
                doc['inserted_at'] = today
                keyword_collection.insert_one(doc)

client.close()