#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于更新本竞品微博账号的信息，包括微博数、性别，粉丝数、位置和最新发布时间

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import re
import time
import datetime
from bson.objectid import ObjectId

# 接口配置
basic_url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=100505{}'
more_url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{}_-_INFO'
publish_url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=107603{}'
headers = {'Referer': 'https://m.weibo.cn', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weibo_account

# 其他配置
today = datetime.date.today()
year = today.year
month = today.month
oneday = datetime.timedelta(days = 1)
yestoday = today - oneday


def format_date(time):
    if re.search(r'分钟|小时', time):
        return today
    elif re.search(r'昨天', time):
        return yestoday
    else:
        m = re.match(r'(\d{4})?-?0?(\d{1,2})-0?(\d{1,2})', time)
        if m:
            m_year = int(m.group(1)) if m.group(1) else year
            return datetime.date(m_year, int(m.group(2)), int(m.group(3)))
        else:
            return None

def fetch_info(account, url_templ):
    url = url_templ.format(account['id'])
    r = requests.get(url, headers = headers)
    if r.status_code == 200:
        r = r.json()
        if r['ok'] == 1:
            return r['data']
        else:
            print('%-40s' % ('× 获取失败：{} | {}'.format(account['name'], url)))        
            return None
    else:
        print('%-40s' % ('× 请求失败：{} | {}'.format(account['name'], url)))        
        return None


# 从数据库获取微博账号，获取信息后更新到数据库
accounts = collection.find({'is_relevant': True, 'is_valid': True},\
    {'id': 1, 'name': 1, 'follower_count': 1, 'last_follower_count': 1,'updated_at': 1})
for account in accounts:
    basic_info = fetch_info(account, basic_url_templ)
    if basic_info:
        mblog_count = basic_info['userInfo']['statuses_count']
        gender = basic_info['userInfo']['gender']
        follower_count = basic_info['userInfo']['followers_count']

        if 'updated_at' in account and 'follower_count' in account:
            if not account['updated_at'][:7] == str(today)[:7]:
                account['last_follower_count'] = account['follower_count']
        else:
            account['last_follower_count'] = 0

        collection.update_one(
            {'_id': ObjectId(account['_id'])},
            {'$set':{
                'mblog_count': mblog_count,
                'gender': gender,
                'follower_count':follower_count,
                'last_follower_count': account['last_follower_count'],
                'updated_at': str(today)
            }})

    time.sleep(3)

    more_info = fetch_info(account, more_url_templ)
    if more_info:
        cards = more_info['cards'][1]['card_group']
        location = cards[len(cards) - 1]['item_content']
        items = location.split()
        if len(items) == 1:
            collection.update_one(
                {'_id': ObjectId(account['_id'])},
                {'$set': {
                    'location': location,
                    'first_region': items[0],
                }})
        else:
            collection.update_one(
                {'_id': ObjectId(account['_id'])},
                {'$set': {
                    'location': location,
                    'first_region': items[0],
                    'second_region': items[1]
                }})

    time.sleep(3)

    publish_info = fetch_info(account, publish_url_templ)
    latest_publish_at = None
    if publish_info:
        cards = publish_info['cards']
        t = None

        # card_type 为 9 的才是微博，取置顶微博（如果有的话）和另外一条最新微博，选择二者间较新的发布日期
        for card in cards:
            if card['card_type'] != 9:
                continue
            else:
                mblog = card['mblog']
                if 'title' in mblog:
                    t = format_date(mblog['created_at'])
                    if not t:
                        print(mblog)
                        exit()
                elif t:
                    ct = format_date(mblog['created_at'])
                    t = ct if ct.__gt__(t) else t
                    break
                else:
                    t = format_date(mblog['created_at'])
                    break

        latest_publish_at = t
        if t:
            collection.update_one(
                {'_id': ObjectId(account['_id'])},
                {'$set': {
                    'latest_publish_at': str(t),
                    'updated_at': str(today)
                }})

    print('%-50s' % ('--> 已更新：{}（{}）{}'.format(account['name'], account['id'], latest_publish_at if latest_publish_at else '')), end='\r')

client.close()