#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于更新本竞品公众号 wx_id，biz_info 以及最新发布文章的日期


# 需要 pip 安装的外部库
from pymongo import MongoClient

# 内置库
import json
import time
import datetime
import re
from bson.objectid import ObjectId

# 自定义库
import nr

# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weixin_account

# 其他配置
today = str(datetime.date.today())


# 使用公众号 id 分别请求新榜官网的账号搜索接口和账号详情接口，更新 wx_id、biz_info 和 uuid
info_accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True},
    {'id': 1, 'name': 1, 'wx_id': 1, 'uuid': 1}))

for account in info_accounts:
    print('%-50s' % ('采集中：{}（{}）...'.format(account['name'], account['id'])), end='\r')

    if 'wx_id' not in account:
        info = nr.get_weixin_account_wx_info(account['id'])
        if info:
            collection.update({'_id': ObjectId(account['_id'])}, {'$set': {
                'wx_id': info['wx_id'],
                'biz_info': info['biz_info'],
                'updated_at': today
            }})
        else:
            print('%-50s' % ('--> wx 信息为空：{}（{}）'.format(account['name'], account['id'])))

    if 'uuid' not in account:
        uuid = nr.get_weixin_account_nr_info(account['id'])
        if uuid:
            collection.update({'_id': ObjectId(account['_id'])}, {'$set': {
                'uuid': uuid,
                'updated_at': today
            }})
        else:
            print('%-50s' % ('--> nr 信息为空：{}（{}）'.format(account['name'], account['id'])))
    
    # 每个账号请求间隔 5 秒
    time.sleep(5)


article_accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True, 'uuid': {'$exists': True}},
    {'id': 1, 'name': 1, 'uuid': 1}))

for account in article_accounts:
    

client.close()