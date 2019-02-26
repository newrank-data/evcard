#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于更新本竞品公众号最新发布文章的日期

# 需要 pip 安装的外部库
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
# lxml 需要安装但不用引入

# 内置库
import json
import time
import datetime
import re
from bson.objectid import ObjectId
 
# 接口配置
url_templ = 'https://weixin.sogou.com/weixin?type=1&query={}'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

# 数据库配置
client = MongoClient()
db = client.newrank
account_collection = db.sharecar_weixin_account

# 其他配置
today = str(datetime.date.today())

def get_url(id):
    url = url_templ.format(id)
    r = requests.get(url, headers = headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        els = soup.select('p.tit a')
        if els:
            return els[0]['href']
        else:
            print('>>> 请求太频繁，在 header1 添加 cookie 吧')
            return None
    else:
        print('%-40s' % ('× 请求失败：{}'.format(url)))
        return None

def get_publish_date(url):
    r = requests.get(url, headers = headers)
    if r.status_code == 200:
        m = re.search(r'.*msgList\s\=\s(\{.*\});\n\s+sea', r.text)
        if m == None:
            return None
        else:
            msg_list = json.loads(m.group(1))['list']
            if msg_list:
                return time.strftime('%Y-%m-%d',time.localtime(msg_list[0]['comm_msg_info']['datetime']))
            else:
                return -1
    else:
        return None

# 使用公众号 id 搜索，得出主页链接，在主页获取最新发布文章的日期
# 搜狗微信无法搜索到服务号，需要手动更新发布信息
accounts = account_collection.find({
        'is_relevant': True,
        'is_valid': True,
        'type': 1
    },{'id': 1, 'name': 1})

for account in accounts:
    print('%-50s' % ('采集中：{}（{}）...'.format(account['name'], account['id'])), end='\r')
    url = get_url(account['id'])
    time.sleep(15)

    if url:
        publish_date = get_publish_date(url)
        
        if publish_date and publish_date != -1:
            print('%-50s' % ('--> {}（{}）{}'.format(account['name'], account['id'], publish_date)))
            account_collection.update({
                        '_id': ObjectId(account['_id'])}, {
                            '$set': {
                                'latest_published_at': publish_date,
                                'updated_at': today }})
        elif publish_date == -1:
            print('%-50s' % ('--> 零发布：{}（{}）'.format(account['name'], account['id'])))
        else:
            print('%-50s' % ('× 详情请求失败：({}){}'.format(account['name'], url)))
            client.close()
    else:
        print('%-50s' % ('× 搜索请求失败：（{}）{}'.format(account['name'], url)))

    time.sleep(15)

client.close()