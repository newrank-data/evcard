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


# 使用公众号 id 分别请求新榜官网的账号搜索接口和账号详情接口，补充新账号的 wx_id、biz_info 和 uuid
info_accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True, 'uuid': {'$exists': False}},
    {'id': 1, 'name': 1, 'wx_id': 1, 'uuid': 1}))

for account in info_accounts:
    print('%-50s' % ('采集中：{}（{}）...'.format(account['name'], account['id'])), end='\r')

    if 'wx_id' not in account:
        info = nr.get_weixin_account_wx_info(account['id'])
        if info:
            collection.update(
                {'_id': ObjectId(account['_id'])},
                {'$set': {'wx_id': info['wx_id'],'biz_info': info['biz_info']}})
        else:
            print('%-50s' % ('--> wx 信息为空：{}（{}）'.format(account['name'], account['id'])))
        time.sleep(3)

    if 'uuid' not in account:
        uuid = nr.get_weixin_account_nr_info(account['id'])
        if uuid:
            collection.update({'_id': ObjectId(account['_id'])}, {'$set': {'uuid': uuid}})
        else:
            print('%-50s' % ('--> nr 信息为空：{}（{}）'.format(account['name'], account['id'])))
        time.sleep(3)


article_accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True, 'uuid': {'$exists': True}},
    {'id': 1, 'name': 1, 'uuid': 1}))

for account in article_accounts:
    t = nr.get_weixin_account_latest_publish_time(account['id'], account['uuid'])
    if t:
        print('%-50s' % ('{}（{}）{}'.format(account['name'], account['id'], t)), end='\r')
        collection.update(
            {'_id': ObjectId(account['_id'])},
            {'$set': {'latest_publish_at': t,'updated_at': today}})
    else:
        print('%-50s' % ('--> 发布时间为空：{}（{}）'.format(account['name'], account['id'])))
    time.sleep(3)

print('\n\n√ 更新完成！信息为空的账号请确认是否录入了新榜，发布时间为空的请确认是否需要设为无效')
client.close()