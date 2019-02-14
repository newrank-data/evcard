#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于采集本竞品微博账号在给定日期范围内更新的微博

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup

# 内置库
import json
import time
import datetime
import re

# 接口配置
url_templ = 'https://m.weibo.cn/api/container/getIndex?containerid=107603{}&page={}'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

# 数据库配置
client = MongoClient()
db = client.newrank
account_collection = db.sharecar_weibo_account
mblog_collection = db.sharecar_weibo_mblog

# 其他配置
today = datetime.date.today()
year = today.year
oneday = datetime.timedelta(days = 1)
yestoday = today - oneday
fetch_empty = []

def input_date(prompt):
    flag = False
    matches = None
    while not flag:
        date = input(prompt)
        matches = re.match(r'(\d{4})-0?(\d{1,2})-0?(\d{1,2})', date)
        if matches:
            flag = True
        else:
            print('🙅‍  格式不正确')
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

def fetch_mblog(account):
    mblogs, page, next_flag, empty_flag = [], 1, True, True
    while next_flag:
        print('%-40s' % ('⌛️  采集中：{}|第{}页...'.format(account['name'], page)), end='\r')
        url = url_templ.format(account['id'], page)
        r = requests.get(url, headers = headers)
        if r.status_code == 200:
            r = r.json()
            if r['ok'] == 1:
                page = r['data']['cardlistInfo']['page']
                if page == None:
                    next_flag = False
                cards = r['data']['cards']
                empty_flag = False

                # card_type 为 9 的才是微博
                for card in cards:
                    if card['card_type'] != 9:
                        continue
                    else:
                        mblog = card['mblog']
                        date = format_date(mblog['created_at'])

                        # 判断是否在日期范围内，由于置顶的可能是上个月的，不能让其改变 flag
                        if date.__ge__(start_date) and date.__le__(end_date):
                            mblogs.append({
                                'id': account['id'],
                                'bid': mblog['bid'],
                                'date': str(date),
                                'content': BeautifulSoup(mblog['text'], 'lxml').get_text(),
                                'quote_count': mblog['reposts_count'],
                                'comment_count': mblog['comments_count'],
                                'attitude_count': mblog['attitudes_count']
                            })
                        elif date.__lt__(start_date) and 'title' not in mblog:
                            next_flag = False
                            break
            else:
                break
        else:
            print('%-40s' % ('💩  请求失败：{} | {}'.format(account['name'], url)))
            break

        
        # 每一页请求间隔 6 秒
        time.sleep(6)

    # 记录微博数为 0 的账号
    if empty_flag:
        fetch_empty.append('{}（{}）'.format(account['name'], account['id']))
        
    return mblogs
    
# 手动输入日期范围并校验
print('💁  按 yyyy-mm-dd 格式输入日期范围...')
start_date = input_date('开始日期：')
end_date = input_date('结束日期：')
if start_date.__gt__(end_date):
    print('🙅  开始日期不能大于结束日期')
    exit()


# 从数据库读取有效的账号，通过接口获取该账号的微博列表，在日期范围内的插入到数据库
print('\n🚀  开始采集...\n')
accounts = account_collection.find({'is_relevant': True, 'is_valid': True}, {'_id': 0, 'id': 1, 'name': 1})
for account in accounts:
    mblogs = fetch_mblog(account)
    mblog_count = len(mblogs)
    if mblog_count:
        print('%-40s' % ('👉  {}（{}）更新了 {} 条微博'.format(account['name'], account['id'], mblog_count)))
        mblog_collection.insert_many(mblogs)
    else:
        print('%-40s' % ('🤷‍  {}（{}）没有更新'.format(account['name'], account['id'])))

# 提示微博数为 0 的账号
if fetch_empty:
    print('\n💁  以下账号从未发布过微博，请确认是否需要在数据库中将其设为无效：')
    print(', '.join(fetch_empty))

client.close()