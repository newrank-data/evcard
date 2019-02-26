#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于采集本竞品微博主账号对应的粉丝账号，一个粉丝只有一条记录，以 following 字段记录其关注的所有品牌主账号

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import time
import datetime
from bson.objectid import ObjectId

# 接口配置
url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{}&type=all&since_id={}'
headers = {'Referer': 'https://m.weibo.cn', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

# 数据库配置
client = MongoClient()
db = client.newrank
account_collection = db.sharecar_weibo_account
follower_collection = db.sharecar_weibo_follower

# 其他配置
today = str(datetime.date.today())

def fetch_follower(account):
    followers, since_id, next_flag = [], 1, True
    while next_flag:
        print('%-40s' % ('⌛️  采集中：{}|第{}页...'.format(account['name'], since_id)), end='\r')
        url = url_templ.format(account['id'], since_id)
        r = requests.get(url, headers = headers)
        if r.status_code == 200:
            r = r.json()
            if r['ok'] == 1:
                cards = r['data']['cards'][-1]['card_group']
                for card in cards:
                    user = card['user']
                    followers.append({'id': str(user['id']), 'name': user['screen_name']})
        else:
            print('%-40s' % ('💩  请求失败：{} | {}'.format(account['name'], url)))        

        since_id += 1
        if since_id == 251:
            break
        time.sleep(5)

    return followers

# 从数据库获取各品牌主账号，分别采集粉丝列表
accounts = account_collection.find({'is_primary': True}, {'_id': 0, 'id': 1, 'name': 1, 'brand': 1 })
for account in accounts:
    followers = fetch_follower(account)
    if followers:
        print('%-40s' % ('👉  {}（{}）采集到 {} 个粉丝'.format(account['name'], account['id'], len(followers))))        
        
        for follower in followers:
            _follower = follower_collection.find_one({'id': follower['id']})
            if _follower:
                # 旧粉丝检查 following 字段是否包含当前品牌
                if account['brand'] not in _follower['following']:
                    _follower['following'].append(account['brand'])
                    follower_collection.update({
                        '_id': ObjectId(_follower['_id'])}, {
                            '$set': {'following': _follower['following']}})
            else:
                follower['following'] = [account['brand']]
                follower['inserted_at'] = today
                follower_collection.insert_one(follower)

client.close()