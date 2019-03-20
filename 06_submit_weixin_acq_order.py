#!/usr/local/bin/python
#coding=utf-8

# 此脚本用于提交公众号回采任务，包括检查榜豆数量、发布日期、添加/删除需要回采的公众号以及提交任务

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import time
import datetime
import re
from bson.objectid import ObjectId

# 自定义库
import nr

# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weixin_account

# 其他配置
start_date, end_date = None, None


# 输入日期校验
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


# 发布时间过滤
def publish_filter(account):
    latest_publish_at = account['latest_publish_at']
    matches = re.match(r'(\d{4})-0?(\d{1,2})-0?(\d{1,2})', latest_publish_at)
    date = datetime.date(int(matches.group(1)), int(matches.group(2)), int(matches.group(3)))
    return date.__ge__(start_date)


# 手动输入日期范围并校验
print('>>> 按 yyyy-mm-dd 格式输入回采日期范围...')
start_date = input_date('开始日期：')
end_date = input_date('结束日期：')
if start_date.__gt__(end_date):
    print('× 开始日期不能大于结束日期')
    exit()


# 获取有效且最新发布文章时间不小于回采开始日期的账号
accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True, 'wx_id': {'$exists': True}, 'latest_publish_at': {'$exists': True}},
    {'id': 1, 'wx_id': 1, 'biz_info': 1, 'name': 1, 'latest_publish_at': 1}))

inserting_accounts = list(filter(publish_filter, accounts))
date_range = (end_date - start_date).days + 1
print('--> 本次任务将回采 {} 个账号，合计 {} 天'.format(len(inserting_accounts), date_range))


# 计算要使用多少榜豆，是否足够
print('\n计算榜豆消耗量...')
bangdou_cost = len(inserting_accounts) * 20 if date_range < 20 else date_range
bangdou_count = nr.count_bangdou()
print('--> 回采需要 {} 榜豆，目前有 {} 榜豆'.format(bangdou_cost, bangdou_count))
if bangdou_cost > bangdou_count:
    client.close()
    print('× 榜豆不足，请充值后再操作！')
    exit()
else:
    print('√ 榜豆足够执行当前回采任务，回采后剩余 {} 榜豆'.format(bangdou_count - bangdou_cost))


# 添加和删除回采账号
deleting_accounts = nr.get_weixin_acq_account()
if deleting_accounts:
    print('\n回采列表将删除 {} 个账号...'.format(len(deleting_accounts)))
    for d_account in deleting_accounts:
        d_res = nr.delete_weixin_acq_account({'uuid':d_account['uuid']})
        print('%-50s' % ('--> 删除：{}（{}）{}'.format(d_account['weixin_name'], d_account['account'], '成功' if d_res == 1 else '失败')))
        time.sleep(1)

if inserting_accounts:
    print('\n回采列表将添加 {} 个账号...'.format(len(inserting_accounts)))
    for i_account in inserting_accounts:
        i_res = nr.insert_weixin_acq_account({
            'account': i_account['id'],
            'biz_info': i_account['biz_info'],
            'weixin_name': i_account['name']
        })
        print('%-50s' % ('--> 添加：{}（{}）{}'.format(i_account['name'], i_account['id'], '成功' if i_res == 1 else '失败')))
        time.sleep(1)


# 提交回采任务
confirm_submit = input('\n>>> 提交回采任务（y/n）：')
if confirm_submit == 'y':
    s_res = nr.submit_weixin_acq_order(str(start_date), str(end_date))
    print('--> 提交结果：{}'.format(s_res)) 
    client.close()
else:
    client.close()
    exit()