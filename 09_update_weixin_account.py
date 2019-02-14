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
headers1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    'Cookie': 'ABTEST=2|1549803206|v1; IPLOC=CN4509; SUID=981289B4771A910A000000005C601EC6; SUID=981289B41620940A000000005C601EC7; weixinIndexVisited=1; SUV=005F1DC2B48912985C60204C3DD7F127; pgv_pvi=5303055360; JSESSIONID=aaahygNN6x7JTa3qTM7Hw; pgv_si=s2559925248; ld=zyllllllll2tYyoxlllllVeOVDklllllHw6vulllllolllllRklll5@@@@@@@@@@; LSTMV=560%2C156; LCLKINT=1506; sct=68; PHPSESSID=nlds2d4qdqj613pa7c4rfb6ru4; SNUID=B01A2E29F7F27620FD93157BF79FBFD8; seccodeRight=success; successCount=1|Thu, 14 Feb 2019 09:16:16 GMT',
    'Host': 'weixin.sogou.com'}

headers2 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    'Host': 'mp.weixin.qq.com'}

# 数据库配置
client = MongoClient()
db = client.newrank
account_collection = db.sharecar_weixin_account

# 其他配置
today = str(datetime.date.today())

def get_url(id):
    url = url_templ.format(id)
    r = requests.get(url, headers = headers1)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        els = soup.select('p.tit a')
        if els:
            return els[0]['href']
        else:
            print('\n' + id)
            return None
    else:
        print('%-40s' % ('💩  请求失败：{}'.format(url)))
        return None

def get_publish_date(url):
    r = requests.get(url, headers = headers2)
    if r.status_code == 200:
        m = re.search(r'.*msgList\s\=\s(\{.*\});\n\s+sea', r.text)
        if m == None:
            return None
        else:
            msg_list = json.loads(m.group(1))['list']
            if msg_list:
                return time.strftime('%Y-%m-%d',time.localtime(msg_list[0]['comm_msg_info']['datetime']))
            else:
                return None
    else:
        return None

# 使用公众号 id 搜索，得出主页链接，在主页获取最新发布文章的日期
# 搜狗微信无法搜索到服务号，需要手动更新发布信息
accounts = account_collection.find({
        'is_relevant': True,
        'is_valid': True,
        'updated_at': {'$exists': False},
        'type': 1,
    },{'id': 1, 'name': 1})
for account in accounts:
    print('%-40s' % ('⌛️  采集中：{}（{}）...'.format(account['name'], account['id'])), end='\r')
    url = get_url(account['id'])
    time.sleep(15)

    if url:
        publish_date = get_publish_date(url)
        
        if publish_date:
            print('%-50s' % ('👉  {}（{}）{}'.format(account['name'], account['id'], publish_date)), end='\r')
            account_collection.update({
                        '_id': ObjectId(account['_id'])}, {
                            '$set': {
                                'latest_published_at': publish_date,
                                'updated_at': today }})
        else:
            print('%-50s' % ('💩  详情请求失败：{}'.format(url)))
            client.close()
    else:
        print('%-50s' % ('💩  搜索请求失败：{}'.format(url)))

    time.sleep(15)

client.close()