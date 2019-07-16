#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 此脚本用于提交微博回采任务，包括检查榜豆数量、发布日期以及提交任务

# 需要 pip 安装的外部库
import requests
from pymongo import MongoClient

# 内置库
import time
import datetime
import re
import json
from bson.objectid import ObjectId

# 自定义库
import nr

# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weibo_account

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
print('>>> 按 yyyy-mm-dd 格式输入回采日期范围：')
start_date = input_date('开始日期：')
end_date = input_date('结束日期：')
if start_date.__gt__(end_date):
    print('× 开始日期不能大于结束日期')
    exit()


# 获取有效且最新发布文章时间不小于回采开始日期的账号
accounts = list(collection.find(
    {'is_relevant': True, 'is_valid': True, 'latest_publish_at': {'$exists': True}},
    {'id': 1, 'name': 1, 'latest_publish_at': 1}))

inserting_accounts = list(filter(publish_filter, accounts))
date_range = (end_date - start_date).days + 1


# 添加回采账号并提交回采任务
if inserting_accounts:

    # 检查当前账号的购物车内是否有账号，有的话先清空
    all_list = nr.get_cart_all_list('3')
    if len(all_list) > 0:
        print('\n--> 购物车不为空，共有 {} 个账号，先清空...'.format(len(all_list)))
        cart_count = nr.empty_cart_list('3')

    # 通过搜索接口确认回采账号是否入库，未入库的进行提示
    print('\n--> 检查回采账号是否入库...')
    included_accounts = []
    excluded_accounts = []
    for a in inserting_accounts:
        search_list = nr.search_weibo_account(a['id'])
        is_included = False

        for l in search_list:
            if l['accountId'] == a['id']:
                print('%-40s' % ('√ {} {}'.format(a['id'], a['name'])), end='\r')
                included_accounts.append(l)
                is_included = True
                break

        if not is_included:
            excluded_accounts.append(a)
            print('%-40s' % ('× {} {}'.format(a['id'], a['name'])), end='\r')
        
        time.sleep(1)
    
    # 如果有未入库的账号，提示等待入库，10 分钟后再运行，自动退出程序
    if len(excluded_accounts) > 0:
        print('\n\n--> 以下 {} 个账号未入库，请等待 10 分钟左右再运行，程序退出'.format(len(excluded_accounts)))
        print(list(map(lambda i: i['id'] + '|' + i['name'] , excluded_accounts)))
        client.close()
        exit()
    
    # 已确认入库的回采账号添加到购物车
    if len(included_accounts) > 0:
        print('\n\n--> 已入库待回采账号共 {} 个，添加到购物车...'.format(len(included_accounts)))
        for account in included_accounts:
            insert_result = nr.insert_cart_account(account, '3')
            print('%-40s' % (account['accountId'] + '|' + account['accountName'] + '|' + insert_result['cartId'][:6]), end='\r')
            time.sleep(1)

        # 抽出账号id，拼接为字符串用于提交回采任务
        accountIds = ','.join(list(map(lambda i: i['accountId'], included_accounts)))
        print('\n\n--> 本次任务将回采 {} 个账号，合计 {} 天'.format(len(included_accounts), date_range))

        # 计算要使用多少榜豆，是否足够
        print('--> 计算榜豆消耗量...')
        bangdou_cost = len(included_accounts) * 15 if date_range < 30 else ((date_range - 30) * 0.5 + 15) * len(included_accounts)
        bangdou_count = nr.count_bangdou()
        print('--> 回采需要 {} 榜豆，目前有 {} 榜豆'.format(bangdou_cost, bangdou_count))
        if bangdou_cost > bangdou_count:
            client.close()
            print('× 榜豆不足，请充值后再操作')
            exit()
        else:
            print('√ 榜豆足够执行当前回采任务，回采后将剩余 {} 榜豆'.format(bangdou_count - bangdou_cost))

        # 提交回采任务
        confirm_submit = input('\n>>> 确认提交回采任务（y/n）：')
        if confirm_submit == 'y':
            order_id = nr.submit_weibo_acq_order(accountIds, str(start_date), str(end_date))
            print('order_id:', order_id)
            pay_status = nr.pay_acq_order(order_id)
            print('pay_status:', pay_status)
            client.close()
        else:
            client.close()
            print('\n--> 回采任务被取消，程序退出')
    else:
        print('\n--> 已入库待回采账号数量为 0，程序退出')
else:
    client.close()
    print('\n--> 回采账号数量为 0，程序退出')