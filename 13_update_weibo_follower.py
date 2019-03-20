#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于更新粉丝账号的信息，包括微博数、性别，粉丝数和位置

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import time
import datetime
from bson.objectid import ObjectId

# 接口配置
basic_url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=100505{}'
more_url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{}_-_INFO'
headers = {'Referer': 'https://m.weibo.cn', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

# 数据库配置
client = MongoClient()
db = client.newrank
follower_collection = db.sharecar_weibo_follower

# 其他配置
today = str(datetime.date.today())

def fetch_info(follower, url_templ):
    url = url_templ.format(follower['id'])
    r = requests.get(url, headers = headers)
    if r.status_code == 200:
        r = r.json()
        if r['ok'] == 1:
            return r['data']
        else:
            print('%-40s' % ('💩  获取失败：{} | {}'.format(follower['name'], url)))        
            return None
    else:
        print('%-40s' % ('💩  请求失败：{} | {}'.format(follower['name'], url)))        
        return None


# 从数据库获取微博账号，获取信息后更新到数据库
followers = list(follower_collection.find({'updated_at': {'$exists': False}}, {'id': 1, 'name': 1}))
for follower in followers:
    print('%-40s' % ('⌛️  采集中：{}（{}）...'.format(follower['name'], follower['id'])), end='\r')

    basic_info = fetch_info(follower, basic_url_templ)
    more_info = fetch_info(follower, more_url_templ)

    if basic_info:
        mblog_count = basic_info['userInfo']['statuses_count']
        gender = basic_info['userInfo']['gender']
        follower_count = basic_info['userInfo']['followers_count']
        follower_collection.update_one({
            '_id': ObjectId(follower['_id'])}, {
                '$set': {
                    'mblog_count': mblog_count,
                    'gender': gender,
                    'follower_count':follower_count,
                    'updated_at': today}})

    time.sleep(1.5)

    if more_info:
        location = more_info['cards'][1]['card_group'][-1]['item_content']
        regions = location.split()
        if len(regions) == 1:
            follower_collection.update_one({
                '_id': ObjectId(follower['_id'])}, {
                    '$set': {'location': location, 'first_region': regions[0]}})
        else:
            follower_collection.update_one({
                '_id': ObjectId(follower['_id'])}, {
                    '$set': {'location': location, 'first_region': regions[0], 'second_region': regions[1]}})

    time.sleep(1.5)
    print('%-40s' % ('👉  {}（{}）信息已更新'.format(follower['name'], follower['id'])))

client.close()