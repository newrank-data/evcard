#! /usr/bin/env python
# _*_ coding: utf-8 _*_

# 此脚本用于整理微信回采的数据，生成用于撰写报告的微信主号相关数据，写入“微信主号统计表_YYYYMM.xlsx”


# 外部库（需要“pip install 库名”进行安装）
import arrow
from pymongo import MongoClient
from openpyxl import Workbook


# 内置库
import os
import re
import calendar
from bson.objectid import ObjectId


# 自定义模块
import nr
import utils


# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weixin_account


# 初始化
tm = arrow.now().shift(months=-1)
lm = arrow.now().shift(months=-2)
day_range = calendar.monthrange(tm.year, tm.month)[1]
label = tm.format('YYYYMM')
last_label = lm.format('YYYYMM')
src_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src')
dist_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'dist')
article_filename = os.path.join(src_path, 'weixin_' + label + '.xlsx')
last_article_filename = os.path.join(src_path, 'weixin_' + last_label + '.xlsx')
regional_accounts = list(collection.find({'is_regional': True}, {'_id': 0, 'id': 1, 'brand': 1, 'region': 1}))
brand_names = ['EVCARD', 'GoFun', '盼达用车', 'car2go', '途歌', '摩范出行','PonyCar', '小桔租车']
regional_brands = ['evcard', 'gofun', 'panda', 'morefun']
type2_accounts = [
    {'id': 'EVCARD', 'name': 'EVCARD服务号'},
    {'id': 'Gofunchuxing', 'name': 'GoFun出行'},
    {'id': 'PAND_AUTO', 'name': '盼达用车'},
    {'id': 'car2gocn', 'name': '即行car2go'},
    {'id': 'gh_1ba806afc3cd', 'name': '摩范MOREFUN'},
    {'id': 'weponycar2016', 'name': 'PONYCAR马上用车'}]
primary_accounts = [
    {'id': 'EVCARD', 'name': 'EVCARD服务号', 'brand_name': 'EVCARD'},
    {'id': 'Gofunchuxing', 'name': 'GoFun出行', 'brand_name': 'GoFun'},
    {'id': 'PAND_AUTO', 'name': '盼达用车', 'brand_name': '盼达用车'},
    {'id': 'car2gocn', 'name': '即行car2go', 'brand_name': 'car2go'},
    {'id': 'mytogo', 'name': 'TOGO途歌', 'brand_name': '途歌'},
    {'id': 'gh_13d453a4ee1c', 'name': '摩范出行', 'brand_name': '摩范出行'},
    {'id': 'weponycar2016', 'name': 'PONYCAR马上用车', 'brand_name': 'PonyCar'},
    {'id': 'didigongxiangqiche', 'name': '小桔租车平台', 'brand_name': '小桔租车'}]


# ———————————— 方法 ————————————


# 过滤服务号
def filter_type2(item):
    flag = True
    for a in type2_accounts:
        if a['id'] == item['id']:
            flag = False
            break
    return flag


# 筛选主号
def filter_main(item):
    flag = False
    for a in primary_accounts:
        if a['id'] == item['id']:
            flag = True
            break
    return flag


# 按账号汇总发布情况
def summarize_publish_by_account(id, articles):
    article_count, read_sum, read_max, read_avg, headline_read_sum, like_sum, like_avg, comment_sum,\
    comment_avg, comment_like_sum, comment_like_avg, comment_reply_sum = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    for a in articles:
        if id == a['id']:
            article_count += 1
            read_sum += a['read']
            read_max = a['read'] if a['read'] > read_max else read_max
            headline_read_sum += a['read'] if a['is_head'] == '是' else 0
            like_sum += a['like']
            comment_sum += a['comment']
            comment_like_sum += a['comment_like']
            comment_reply_sum += a['comment_reply']
    read_avg = read_sum / article_count if article_count > 0 else 0
    like_avg = like_sum / article_count if article_count > 0 else 0
    comment_avg = comment_sum / article_count if article_count > 0 else 0
    comment_like_avg = comment_like_sum / article_count if article_count > 0 else 0
    comment_reply_ratio = str(utils.round_decimal(comment_reply_sum / comment_sum * 100)) + '%' if comment_sum > 0 else '0.0%'

    return { 'article_count': article_count, 'read_sum': read_sum, 'read_max': read_max, 'read_avg': read_avg,\
    'headline_read_sum': headline_read_sum, 'like_sum': like_sum,'like_avg': utils.round_decimal(like_avg),\
    'comment_avg': utils.round_decimal(comment_avg), 'comment_like_avg': utils.round_decimal(comment_like_avg),\
    'comment_reply_ratio': comment_reply_ratio}


# 按品牌汇总发布情况
def summarize_publish_by_brand(brand_name, articles):
    article_count, read_sum = 0, 0
    for a in articles:
        article_brand_name = utils.match_brand(a['name'])
        if article_brand_name == brand_name:
            article_count += 1
            read_sum += a['read']
    return { 'article_count': article_count, 'read_sum': read_sum}


# 按地区汇总发布情况
def summarize_publish_by_region(articles):
    regions = []
    for article in articles:
        is_new = True
        for region in regions:
            if region['region'] == article['region']:
                region['article_count'] += 1
                region['read_sum'] += article['read']
                is_new = False
                break
        if is_new:
            regions.append({'region': article['region'], 'article_count': 1, 'read_sum': article['read']})
    regions.sort(key=lambda i: i['article_count'], reverse=True)
    return regions


# 匹配品牌和区域
def match_brand_and_region(item):
    for index, a in enumerate(regional_accounts):
        if item['id'] == a['id']:
            item['brand'] = a['brand']
            item['region'] = a['region']
            break
    return item

# ———————————— 处理 ————————————

# 创建工作簿
wb = Workbook()
ws = wb.active
ws.title = '微信主号'

articles = utils.get_articles(article_filename)
last_articles = utils.get_articles(last_article_filename)


# 服务号数据概览
ws.append(['公众号名称', '发文数', '发文数环比增长', '阅读数', '阅读数环比增长'])
for account in type2_accounts:
    publish_summary = summarize_publish_by_account(account['id'], articles)
    last_publish_summary = summarize_publish_by_account(account['id'], last_articles)
    article_count_relative = utils.calc_relative_ratio_1(last_publish_summary['article_count'], publish_summary['article_count'])
    read_sum_relative = utils.calc_relative_ratio_1(last_publish_summary['read_sum'], publish_summary['read_sum'])
    ws.append([account['name'], publish_summary['article_count'], article_count_relative,\
        publish_summary['read_sum'], read_sum_relative])


# 订阅号数据概览
ws.append([])
ws.append(['品牌', '发文数', '发文数环比增长', '阅读数', '阅读数环比增长'])
for brand_name in brand_names:
    publish_summary = summarize_publish_by_brand(brand_name, list(filter(filter_type2, articles)))
    last_publish_summary = summarize_publish_by_brand(brand_name, list(filter(filter_type2, last_articles)))
    article_count_relative = utils.calc_relative_ratio_1(last_publish_summary['article_count'], publish_summary['article_count'])
    read_sum_relative = utils.calc_relative_ratio_1(last_publish_summary['read_sum'], publish_summary['read_sum'])
    ws.append([brand_name, publish_summary['article_count'], article_count_relative,\
        publish_summary['read_sum'], read_sum_relative])


# 区域数据对比
regional_articles = list(map(match_brand_and_region, articles))
for brand in regional_brands:
    ws.append([])
    ws.append([brand, '发文数', '阅读数'])
    brand_regional_articles = list(filter(lambda i: 'region' in i and i['brand'] == brand, regional_articles))
    region_publish_summary = summarize_publish_by_region(brand_regional_articles)
    for s in region_publish_summary:
        ws.append([s['region'], s['article_count'], s['read_sum']])


# 汇总主号发布数据
for a in primary_accounts:
    a['publish_summary'] = summarize_publish_by_account(a['id'], articles)
    a['last_publish_summary'] = summarize_publish_by_account(a['id'], last_articles)
    a['nri'] = utils.round_decimal(nr.calc_nri(a['publish_summary'], day_range))


# 主号发文数、阅读数、发文数环比、阅读数环比
ws.append([])
ws.append(['账号名称', label + '发文数', label +'阅读数', '发文数环比', '阅读数环比', '新榜指数'])
for a in primary_accounts:
    publish_summary = a['publish_summary']
    last_publish_summary = a['last_publish_summary']
    article_count_relative = utils.calc_relative_ratio_1(last_publish_summary['article_count'], publish_summary['article_count'])
    read_sum_relative = utils.calc_relative_ratio_1(last_publish_summary['read_sum'], publish_summary['read_sum'])
    ws.append([a['name'], publish_summary['article_count'], publish_summary['read_sum'],\
        article_count_relative, read_sum_relative])


# 主号新榜指数
ws.append([])
ws.append(['EVCARD','GoFun','盼达用车','car2go','途歌','摩范出行','PonyCar','小桔租车'])
nris = list(map(lambda i: i['nri'], primary_accounts))
ws.append(nris)


# TOP20 阅读文章
ws.append([])
ws.append(['#', '账号名称', '标题', '阅读数'])
main_articles = list(filter(filter_main, articles))
main_articles.sort(key=lambda a: a['read'], reverse=True)
for i, a in enumerate(main_articles[:20]):
    ws.append([i + 1, a['name'], a['title'], a['read']])


# 主号平均点赞数、平均评论数、平均评论点赞数
ws.append([])
ws.append(['','平均点赞数','平均评论数','平均评论点赞数'])
for a in primary_accounts:
    if a['id'] in ['EVCARD','Gofunchuxing','PAND_AUTO','gh_13d453a4ee1c','didigongxiangqiche']:
        p = a['publish_summary']
        ws.append([a['name'], p['like_avg'], p['comment_avg'], p['comment_like_avg']])


# 主号评论回复率
ws.append([])
ws.append(['EVCARD','GoFun','盼达用车','car2go','途歌','摩范出行','PonyCar','小桔租车'])
comment_reply_ratios = list(map(lambda i: i['publish_summary']['comment_reply_ratio'], primary_accounts))
ws.append(comment_reply_ratios)


wb.save(os.path.join(dist_path, '微信主号统计表_{}.xlsx'.format(label)))
print('\n完成！结果文件保存在 dist 目录下')