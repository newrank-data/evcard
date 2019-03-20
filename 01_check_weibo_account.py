#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于检测本竞品是否有新增的微博账号
# 通过品牌关键词搜索相关账号，与数据库已有账号比对，如果有新增账号则提示并插入

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import json
import time
import datetime

# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weibo_account

# 其他配置
url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D{}%26t%3D0&page_type=searchall&page={}'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
new_account_count = 0
brands = [
    {'name': 'evcard', 'keyword': 'evcard'},
    {'name': 'gofun', 'keyword': 'gofun'},
    {'name': 'panda', 'keyword': '盼达用车'},
    {'name': 'car2go', 'keyword': 'car2go'},
    {'name': 'morefun', 'keyword': '摩范出行'},
    {'name': 'ponycar', 'keyword': 'ponycar'},
    {'name': 'xiaoju', 'keyword': '小桔租车'}]
today = str(datetime.date.today())

def assemble(item):
    return {'id': str(item['user']['id']), 'name': item['user']['screen_name']}

for brand in brands:
    page = 1
    users = []

    while page:
        url = url_templ.format(brand['keyword'], page)
        r = requests.get(url, headers = headers)
        print('%-40s' % ('采集中：{} | 第 {} 页...'.format(brand['name'], page)), end='\r')

        if r.status_code == 200:
            r = r.json()
            if r['ok'] == 1:
                cards = r['data']['cards'][-1]['card_group']

                # 提取数据组装成 {id, name}，并入 users
                users += map(assemble, cards)
                
                # 如果是最后一页，会返回 page = null，结束 while 循环
                page = r['data']['cardlistInfo']['page']
        else:
            print('× 请求失败', url)

        # 每一页请求间隔 5 秒
        time.sleep(5)

    print('%-40s' % ('采集完成：' + brand['name']))

    # 与数据库中同品牌的账号进行比对
    accounts = list(collection.find({'brand': brand['name']}, {'_id': 0, 'id': 1, 'name': 1}))
    for user in users:
        flag = False
        for account in accounts:
            if user['id'] == account['id']:
                flag = True
                break
        if not flag:
            new_account_count += 1
            print('%-60s' % ('--> 新账号：{}|{}|{}|'.format(brand['name'], user['id'], user['name'])))

            # 更新到数据库，待定属性设为为空值
            collection.insert_one({
                'brand': brand['name'],
                'id': user['id'],
                'name': user['name'],
                'is_relevant': None,
                'is_valid': None,
                'is_primary': None,
                'is_regional': None,
                'region': None,
                'inserted_at': today
                })

if new_account_count:
    print('\n√ 检测完成！有 {} 个新账号，请在数据库中确认空值属性'.format(new_account_count))
else:
    print('\n√ 检测完成！没有发现新账号')

client.close()