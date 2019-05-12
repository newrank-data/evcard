#!/usr/bin/env python3
#coding=utf-8

# 此模块用于进行新榜官网上的各类操作
# 操作需要有效的用户名、密码以及加密参数，存放于模块根目录下 settings.json，如果没有请按以下格式自行创建
# {
#     "username": "账号（邮箱）",
#     "password": "密码",
#     "md_key": "daddy",
#     "app_key": "joker"
# }

# 需要 pip 安装的外部库
import requests
from bs4 import BeautifulSoup

# 内置库
import os
import json
import time
import random
import hashlib
import re
import math

# 其他配置
username, password, md_key, app_key = '', '', '', ''


# 初始化，读取 settings 获取用户名及密码
file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'settings.json')
with open(file_path, 'r') as f:
    s = json.load(f)
    username, password, md_key, app_key = s['username'], s['password'], s['md_key'], s['app_key']


# 封装 md5
def md5(str):
    m = hashlib.md5()
    m.update(str.encode(encoding='utf-8'))
    return m.hexdigest()


# 密码加密
def  encrypt_password(str):
    return md5(md5(str) + md_key)


# 生成随机数 nonce
def get_nonce():
    factor = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
    nonce = ''
    for i in range(9):
        nonce += factor[int(random.random() * 16)]
    return nonce


# 参数重新排序，加上 nonce 并计算客户端盐 xyz，组装后返回
def assemble_data(endpoint, params):
    data = {}
    url = '/xdnphb' + endpoint + '?AppKey=' + app_key
    keys = list(params.keys())
    keys.sort()

    for key in keys:
        data[key] = params[key]
        url += '&{}={}'.format(key, params[key])

    nonce = get_nonce()
    data['nonce'] = nonce
    url += '&nonce={}'.format(nonce)
    data['xyz'] = md5(url)
    return data


# 生成时间戳
def get_flag():
    timestamp = str(int(time.time() * 1000))
    random_number = '%.17f' % (random.random())
    return timestamp + random_number


# 获取 token，优先从本地获取，超过 15 分钟再重新请求
def get_token():
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'token')
    file_existence = os.path.exists(file_path)

    if file_existence:
        file_mtime = os.stat(file_path).st_mtime
        now = int(time.time())
        if now - file_mtime > 900:
            update_token()    
    else:
        update_token()

    with open(file_path, 'r') as f:
        return f.readline()


# 更新 token
def update_token():
    endpoint = '/login/new/usernameLogin'
    data = assemble_data(endpoint, {
        'flag': get_flag(),
        'identifyCode': '',
        'username': username,
        'password': encrypt_password(password)
    })
    r = requests.post('https://www.newrank.cn/xdnphb' + endpoint, data=data)
    if r.status_code == 200:
        res = r.json()
        if isinstance(res['value'], dict):
            token = res['value']['token']
            file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'token')
            with open(file_path, 'w') as f:
                f.write(token)
        else:
            print(res)
            print('登录验证失败')
            exit()
    else:
        print('登录请求失败')
        exit()

 
# 发起请求
def request(base, endpoint, params):
    token = get_token()
    data = assemble_data(endpoint, params)
    r = requests.post(base + endpoint, cookies = {'token': token}, data = data)
    if r.status_code == 200:
        r = r.json()
        if isinstance(r['value'], (dict, list)) or r['value'] == 1:
            return r['value']
        else:
            print('验证失败：', endpoint, data, r)
            return None
    elif r.status_code == 504:
        return None
    else:
        print('请求失败：', endpoint, data, r, token)
        return None



# 获取榜豆数量
def count_bangdou():
    base = 'https://www.newrank.cn/xdnphb'
    endpoint = '/user/bangdou/consumeDetailAndBangDou'
    r = request(base, endpoint, {'pageNum': 0})
    return int(float(r['bangdou']))


# 搜索微信上的公众号信息（结果可能是 0 到任意个）
def search_weixin_account_wx_info(keyword):
    base = 'https://www.newrank.cn/xdnphb'
    endpoint = '/data/weixinuser/searchWeixinDataByCondition'
    
    # 接口不稳定，需要有重试机制
    r, accounts, retry_count = None, [], 0
    while not r:
        if retry_count > 0:
            print('重试第 {} 次'.format(retry_count))
        r = request(base, endpoint, {
                'filter': '',
                'hasDeal': 'false',
                'keyName': keyword,
                'order': 'relation'
            })
        retry_count += 1
        time.sleep(5)

    if r['result']:
        for v in r['result']:
            accounts.append({
                'id': v['account'],
                'wx_id': v['wxIdLower'],
                'biz_info': v['bizInfo'],
                'name': re.sub(r'[\@|\#]font', '', v['name']),
                'type': v['accountType'],
                'category': v['type'],
                'tags': v['tags'],
                'uuid': v['uuid'],
                'certification': re.sub(r'微信认证：', '', v['certifiedText']) if v['certifiedText'] else None
            })
    return accounts

# 获取微信上的公众号信息（结果可能是 0 到 1 个，完全匹配 id）
def get_weixin_account_wx_info(id):
    accounts = search_weixin_account_wx_info(id)
    info = None
    if accounts:
        for account in accounts:
            if account['id'] == id:
                info =  {
                    'id': id,
                    'wx_id': account['wx_id'],
                    'biz_info': account['biz_info']
                }
                break
    return info

# 获取新榜上的公众号信息（uuid）
def get_weixin_account_nr_info(id):
    url = 'https://www.newrank.cn/public/info/detail.html?account={}'.format(id)
    r = requests.get(url, cookies={'token': get_token()})
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        link = soup.select('.more a')
        if link:
            return link[0]['href'][-32:]
        else:
            return None
    else:
        return None


# 获取公众号最新发布文章时间
def get_weixin_account_latest_publish_time(uuid):
    base = 'https://www.newrank.cn/xdnphb'
    endpoint = '/detail/getAccountArticle'

    # 接口不稳定，需要有重试机制
    r, t = None, None
    while not r:
        r = request(base, endpoint, {'flag': 'true', 'uuid': uuid})
        if r and 'lastestArticle' in r and r['lastestArticle']:
            t =  r['lastestArticle'][0]['publicTime'][:10]
            break
    return t


# 获取回采公众号账号
def get_weixin_acq_account():
    base = 'https://data.newrank.cn/xdnphb'
    endpoint = '/app/data/dataacq/getDataAcqOrder'
    r = request(base, endpoint, {})
    return r


# 获取公众号账号列表差集
def diff_weixin_account(list_1, list_2):
    diff_list = []
    for i in list_1:
        flag = False
        for j in list_2:
            if i['biz_info'] == j['biz_info']:
                flag = True
                break
        if not flag:
            diff_list.append(i['biz_info'])
    return diff_list


# 增加回采公众号账号
def insert_weixin_acq_account(account):
    base = 'https://data.newrank.cn/xdnphb'
    endpoint = '/app/data/dataacq/insertDataAcqOrder'
    r = request(base, endpoint, account)
    return 1 if r['status'] == 1 else 0


# 删除回采公众号账号
def delete_weixin_acq_account(account):
    base = 'https://data.newrank.cn/xdnphb'
    endpoint = '/app/data/dataacq/delDataAcqOrder'
    r = request(base, endpoint, account)
    return 1 if r == 1 else 0


# 提交公众号回采任务
def submit_weixin_acq_order(start_date, end_date):
    base = 'https://data.newrank.cn/xdnphb'
    endpoint = '/app/data/dataacq/inserAcqTask'
    r = request(base, endpoint, {
        'all_data': 0,
        'start_time': start_date + ' 00:00:00',
        'end_time': end_date + ' 00:00:00',
        'is_include': 1
    })
    return r['status']


# 提交微博回采任务
def submit_weibo_acq_order(item):
    base = 'https://data.newrank.cn/xdnphb'
    endpoint = '/app/data/weibodataacq/insertTask'
    r = request(base, endpoint, {'item': item})
    return r['status']


# 计算新榜指数
def calc_nri(summary, n):
    if summary['article_count'] == 0:
        return 0
    else:
        R, Rm, Ra, Rh, Z = n * 800000, 100000, 100000, n * 100000, n * 80000
        _R = math.log(summary['read_sum'] + 1) / math.log(R + 1) * 1000
        _Rm = math.log(summary['read_max'] + 1) / math.log(Rm + 1) * 1000
        _Ra = math.log(summary['read_avg'] + 1) / math.log(Ra + 1) * 1000
        _Rh = math.log(summary['headline_read_sum'] + 1) / math.log(Rh + 1) * 1000
        _Z = math.log(summary['like_sum'] + 1) / math.log(Z + 1) * 1000
        return _R * 0.75 + _Rm * 0.05 + _Ra * 0.1 + _Rh * 0.05 + _Z * 0.05
