#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于检测本竞品是否有新增的微信公众号账号
# 通过品牌关键词搜索相关账号，与数据库已有账号比对，如果有新增账号则提示并插入

# 需要 pip 安装的外部库
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
# lxml 需要安装但不用引入

# 内置库
import json
import time


url_templ = 'https://weixin.sogou.com/weixin?type=1&query={}&ie=utf8&s_from=input&_sug_=y&_sug_type_=&page={}'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
brands = [{'name': 'evcard', 'keyword': 'evcard'},
    {'name': 'gofun', 'keyword': 'gofun'},
    {'name': 'panda', 'keyword': '盼达用车'},
    {'name': 'car2go', 'keyword': 'car2go'},
    {'name': 'morefun', 'keyword': '摩范出行'},
    {'name': 'ponycar', 'keyword': 'ponycar'}]

# 从数据库读取已有账号
client = MongoClient()
db = client.newrank
collection = db.sharecar_weixin_account

def extract(item):
    return item.get_text()

def assemble(item):
    return {'id': str(item['user']['id']), 'name': item['user']['screen_name']}

for brand in brands:
    page = 1
    users = []

    while page:
        url = url_templ.format(brand['keyword'], page)
        r = requests.get(url, headers = headers)

        if r.status_code == 200:
            print('%-20s' % ('⏳  采集中：{}|第{}页...'.format(brand['name'], page)), end='\r')
            soup = BeautifulSoup(r.text, 'lxml')

            # 提取数据组装成 {id, name}，并入 users
            ids = list(map(extract, soup.select('label[name="em_weixinhao"]')))
            names = list(map(extract, soup.select('p.tit a')))
            for i in range(0, len(ids)):
                users.append({'id': ids[i], 'name': names[i]})

            # 如果是最后一页，下一页按钮为空，令 page = None，结束 while 循环
            np = soup.select('a.np')
            page = page + 1 if np else None
        else:
            print('请求失败', url)

        # 每一页请求间隔 3 秒
        time.sleep(3)
    
    print('%-25s' % ('采集完成：' + brand['name']))

    # 与数据库中同品牌的账号进行比对
    accounts = list(collection.find({'brand': brand['name']}, {'_id': 0, 'id': 1, 'name': 1}))
    for user in users:
        flag = False
        for account in accounts:
            if user['id'] == account['id']:
                flag = True
                break
        if not flag:
            print('%-60s' % ('👉  新账号：{}|{}|'.format(user['id'], user['name'])))

            # 更新到数据库，待定属性设为为空值
            collection.insert_one({
                'brand': brand['name'],
                'id': user['id'],
                'name': user['name'],
                'type': 1,
                'is_relevant': None,
                'is_valid': None,
                'is_primary': None,
                'is_regional': None,
                'region': None
                })

client.close()
print('\n🎉  检测完成！如果有新账号，请在数据库中确认空值属性')