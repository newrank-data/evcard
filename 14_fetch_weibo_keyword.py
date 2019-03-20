#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 此脚本用于采集提及了本竞品品牌的微博，使用 mentioning 字段记录提及的品牌
# 注意：此脚本使用的微博接口并没有返回所有微博，非特殊需要请使用 Web Scaper 采集 html 页面，再使用 07_handle_weibo_keyword.py 来处理

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup

# 内置库
import json
import time
import datetime
import re
from bson.objectid import ObjectId

# 接口配置
url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D61%26q%3D{}%26t%3D0&page_type=searchall&page={}'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
brands = [
    {'name': 'evcard', 'keyword': 'evcard'},
    {'name': 'gofun', 'keyword': 'gofun'},
    {'name': 'panda', 'keyword': '盼达'},
    {'name': 'car2go', 'keyword': 'car2go'},
    {'name': 'morefun', 'keyword': '摩范出行'},
    {'name': 'ponycar', 'keyword': 'ponycar'}]

# 数据库配置
client = MongoClient()
db = client.newrank
keyword_collection = db.sharecar_weibo_keyword

# 其他配置
today = datetime.date.today()
year = today.year
oneday = datetime.timedelta(days = 1)
yestoday = today - oneday

def input_date(prompt):
    flag = False
    matches = None
    while not flag:
        date = input(prompt)
        matches = re.match(r'(\d{4})-0?(\d{1,2})-0?(\d{1,2})', date)
        if matches:
            flag = True
        else:
            print('× 格式不正确')
    return datetime.date(int(matches.group(1)), int(matches.group(2)), int(matches.group(3)))

def format_date(time):
    if re.search(r'分钟|小时', time):
        return today
    elif re.search(r'昨天', time):
        return yestoday
    else:
        m = re.match(r'(\d{4})?-?0?(\d{1,2})-0?(\d{1,2})', time)
        m_year = int(m.group(1)) if m.group(1) else year
        return datetime.date(m_year, int(m.group(2)), int(m.group(3)))

def fetch_mblog(brand):
    mblogs, page, next_flag = [], 1, True
    while next_flag:
        print('%-40s' % ('采集中：{}|第{}页...'.format(brand['name'], page)), end='\r')
        url = url_templ.format(brand['keyword'], page)
        r = requests.get(url, headers = headers)
        if r.status_code == 200:
            r = r.json()
            if r['ok'] == 1:
                page = r['data']['cardlistInfo']['page']

                # 没有下一页时接口会返回 page = null
                if page == None:
                    next_flag = False
                cards = r['data']['cards'][0]['card_group']

                for card in cards:
                    mblog = card['mblog']
                    date = format_date(mblog['created_at'])

                    # 判断是否在日期范围内
                    if date.__ge__(start_date) and date.__le__(end_date):
                        content = ''
                        if mblog['isLongText']:
                            content = mblog['longText']['longTextContent']
                        else:
                            content = BeautifulSoup(mblog['text'], 'lxml').get_text()
                        user = mblog['user']
                        mblogs.append({
                            'id': mblog['id'],
                            'bid': mblog['bid'],
                            'created_at': str(date),
                            'content': content,
                            'quote_count': mblog['reposts_count'],
                            'comment_count': mblog['comments_count'],
                            'attitude_count': mblog['attitudes_count'],
                            'user_id': str(user['id']),
                            'user_name': user['screen_name']
                        })
                    elif date.__lt__(start_date):
                        next_flag = False
                        break
            else:
                print('%-40s' % ('× 获取失败：{} | {}'.format(brand['name'], url)))
                break
        else:
            print('%-40s' % ('× 请求失败：{} | {}'.format(brand['name'], url)))
            break

        time.sleep(5)
        
    return mblogs
    
# 手动输入日期范围并校验
print('>>> 按 yyyy-mm-dd 格式输入日期范围...')
start_date = input_date('开始日期：')
end_date = input_date('结束日期：')
if start_date.__gt__(end_date):
    print('× 开始日期不能大于结束日期')
    exit()


# 通过微博接口采集包含某个关键词的微博列表，在日期范围内的插入到数据库
for brand in brands:
    mblogs = fetch_mblog(brand)
    mblog_count = len(mblogs)
    if mblog_count:
        print('%-40s' % ('--> {}（{}）相关微博有 {} 条'.format(brand['name'], brand['keyword'], mblog_count)))

        for mblog in mblogs:
            _mblog = keyword_collection.find_one({'id': mblog['id']})
            if _mblog:
                # 已存在的微博检查 mentioning 字段是否包含当前品牌
                if brand['name'] not in _mblog['mentioning']:
                    _mblog['mentioning'].append(brand['name'])
                    keyword_collection.update({
                        '_id': ObjectId(_mblog['_id'])}, {'$set': {'mentioning': _mblog['mentioning']}})
            else:
                mblog['mentioning'] = [brand['name']]
                mblog['inserted_at'] = str(today)
                keyword_collection.insert_one(mblog)
    else:
        print('%-40s' % ('--> {}（{}）没有相关微博'.format(brand['name'], brand['keyword'])))

client.close()