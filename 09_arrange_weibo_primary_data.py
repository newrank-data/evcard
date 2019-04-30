#! /usr/bin/env python
# _*_ coding: utf-8 _*_

# 此脚本用于整理微博回采的数据，生成用于撰写报告的微博主号相关数据，写入 weibo_YYYYMM_primary.xlsx


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
import utils


# 数据库配置
client = MongoClient()
db = client.newrank
collection = db.sharecar_weibo_account


# 初始化
label = arrow.now().shift(months=-1).format('YYYYMM')
last_label = arrow.now().shift(months=-2).format('YYYYMM')
src_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src')
dist_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'dist')
article_filename = os.path.join(src_path, 'weibo_' + label + '.xlsx')
last_article_filename = os.path.join(src_path, 'weibo_' + last_label + '.xlsx')
follower_filename = os.path.join(src_path, 'weibo_follower_' + label + '.xlsx')
last_follower_filename = os.path.join(src_path, 'weibo_follower_' + last_label + '.xlsx')
mention_filename = os.path.join(src_path, 'weibo_mention_' + label + '.xlsx')
last_mention_filename = os.path.join(src_path, 'weibo_mention_' + last_label + '.xlsx')
primary_infos = list(collection.find({'is_primary': True}, {'_id': 1, 'id': 1, 'brand': 1, 'follower_count': 1, 'last_follower_count': 1}))
officials = list(collection.find({'is_relevant': True}, {'_id': 0, 'id': 1}))
gofun_ambiguous_words = utils.get_ambiguous_words('gofun')
panda_ambiguous_words = utils.get_ambiguous_words('panda')
togo_ambiguous_words = utils.get_ambiguous_words('togo')


primarys = [
    {'brand': 'evcard', 'brand_name': 'EVCARD', 'name': 'EVCARD官方微博', 'keyword': ['evcard']},
    {'brand': 'gofun', 'brand_name': 'GoFun', 'name': 'GoFun出行', 'keyword': ['gofun']},
    {'brand': 'panda', 'brand_name': '盼达用车', 'name': '盼达用车', 'keyword': ['盼达']},
    {'brand': 'car2go', 'brand_name': 'car2go', 'name': '即行car2go', 'keyword': ['car2go']},
    {'brand': 'togo', 'brand_name': '途歌', 'name': 'TOGO途歌', 'keyword': ['途歌']},
    {'brand': 'morefun', 'brand_name': '摩范出行', 'name': '摩范出行', 'keyword': ['摩范出行']},
    {'brand': 'ponycar', 'brand_name': 'PonyCar', 'name': 'PonyCar马上用车', 'keyword': ['ponycar']},
    {'brand': 'xiaoju', 'brand_name': '小桔租车', 'name': '小桔租车官方微博', 'keyword': ['小桔租车', '滴滴共享汽车']}]
 

for p in primarys:
    for i in primary_infos:
        if p['brand'] == i['brand']:
            p['id'] = i['id']
            p['follower_count'] = i['follower_count']
            p['last_follower_count'] = i['last_follower_count']
            break


# ———————————— 方法 ————————————


# 精准四舍五入保留 1 位小数
def round_decimal(value):
	m = re.search(r'\.(\d)5$', str(value))
	if m and int(m.group(1)) % 2 == 0:
		return round(value, 1) + 0.1
	else:
		return round(value * 1.0, 1)


# 主号发文筛选器
def primary_filter(item):
    flag = False
    for p in primarys:
        if p['id'] == item['id']:
            flag = True
            break
    return flag


# 按品牌汇总发文及互动
def summarize_primary(articles):
    summary = []
    for p in primarys:
        article_count, interact_sum = 0, 0
        for a in articles:
            if a['id'] == p['id']:
                article_count += 1
                interact_sum += (a['quote'] + a['like'] + a['comment'])
        summary.append({
            'brand_name': p['brand_name'],
            'article_count': article_count,
            'interact_sum': interact_sum
        })
    return summary


# 计算变化幅度，保留 1 位小数
def calc_relative_ratio_1(v1, v2):
    if v1 == 0 and v2 == 0:
        return '-'
    elif v1 == 0:
        return '+∞'
    else:
        return str(round_decimal((v2 - v1) / v1 * 100)) + '%'


# 计算变化幅度，保留 2 位小数
def calc_relative_ratio_1(v1, v2):
    if v1 == 0 and v2 == 0:
        return '-'
    elif v1 == 0:
        return '+∞'
    else:
        return str(round((v2 - v1) / v1 * 100, 2)) + '%'


# 计算粉丝男女占比
def calc_follower_gender_percentage(followers):
    percentages = []
    for p in primarys:
        female_count, male_count, follower_count = 0, 0, 0
        for f in followers:
            if p['id'] == f['from']:
                follower_count += 1
                female_count += 1 if f['gender'] == 'f' else 0
        if follower_count:
            female_percentage = round_decimal(female_count / follower_count * 100)
            male_percentage = 100 - female_percentage
            percentages.append([p['name'], str(female_percentage) + '%', str(male_percentage) + '%'])
    return percentages


# 计算粉丝地域占比
def calc_region_rank(id, followers):
    followers_count, ranks = 0, []
    for f in followers:
        if f['from'] == id:
            followers_count += 1
            if not f['province'] == '其他':
                is_new = True
                for r in ranks:
                    if r['province'] == f['province']:
                        r['count'] += 1
                        is_new = False
                        break
                if is_new:
                    ranks.append({'province': f['province'], 'count': 1})
    
    for r in ranks:
        r['percentage'] = round_decimal(r['count'] / followers_count * 100)
    
    ranks.sort(key=lambda r: r['count'], reverse=True)
    return(ranks[:10])


def calc_region_rank_offset(rank, last_rank):
    for i, r in enumerate(rank):
        r['offset'] = ''
        is_match = False
        for j, lr in enumerate(last_rank):
            if r['province'] == lr['province']:
                is_match = True
                if i < j:
                    r['offset'] = j - i
                break
        if not is_match:
            r['offset'] = 'new'


def summarize_mention(keywords, mentions):
    mention_count = 0
    authors = []
    for m in mentions:
        if m['keyword'] in keywords:
            mention_count += 1
            is_new = True
            for a in authors:  
                if m['oid'] == a['oid']:
                    is_new = False
                    a['quote_count'] += m['quote_count']
                    a['comment_count'] += m['comment_count']
                    if m['quote_count'] > a['max_quote_count']:
                        a['max_quote_count'] = m['quote_count']
                        a['max_quote_url'] = m['url']
                    if m['comment_count'] > a['max_comment_count']:
                        a['max_comment_count'] = m['comment_count']
                        a['max_comment_url'] = m['url']
            if is_new:
                authors.append({'oid': m['oid'], 'name': m['author'],\
                    'quote_count': m['quote_count'],'comment_count': m['comment_count'],\
                    'max_quote_count': m['quote_count'], 'max_comment_count': m['comment_count'],\
                    'max_quote_url': m['url'], 'max_comment_urld': m['url']})
    authors.sort(key=lambda a: a['quote_count'], reverse=True)
    quote_author_rank = authors[:5]
    authors.sort(key=lambda a: a['comment_count'], reverse=True)
    comment_author_rank = authors[:5]
    return {'mention_count': mention_count, 'quote_quthor_rank': quote_author_rank, 'comment_author_rank': comment_author_rank}


# 过滤官方提及
def filter_offical_mention(item):
    is_official = False
    for o in officials:
        if o['id'] == item['oid']:
            is_official = True
            break
    return not is_official


# 过滤无关提及
def filter_irrelevant_mention(item):
    if item['keyword'] == 'gofun':
        if 'gofun' not in item['content']:
            return False
        elif re.search(r'潮流集成', item['content']):
            return False
        else:
            for w in gofun_ambiguous_words:
                if w in item['content']:
                    return False
            return True
    elif item['keyword'] == '盼达':
        if '盼达' not in item['content']:
            return False
        elif re.search(r'盼达用车|盼达租车|盼达汽车', item['content']):
            return True
        elif re.search(r'陈立农|金秀贤|张艺兴|pandakorea|国宝|ppt|成都地铁|3号线|三号线', item['content']):
            return False
        elif re.search(r'车|租|开|押金|退|退费|客服|补贴|app|滴滴|易到|共享|出行|诉|骗|运维|钱|资产|企业',\
            item['content']) and not re.search(r'开始|张开|盛开', item['content']):
            return True
        else:
            for w in panda_ambiguous_words:
                if w in item['content']:
                    return False
            return True
    elif item['keyword'] == '途歌':
        if '途歌' not in item['content']:
            return False
        elif re.search(r'途歌公司|途歌租车|途歌汽车|TOGO途歌', item['content']):
            return True
        elif re.search(r'陈立农|里咏', item['content']):
            return False
        elif re.search(r'车|租|开|押金|退|退费|客服|补贴|app|滴滴|易到|共享|出行|诉|骗|运维|钱|资产|企业', \
            item['content']) and not re.search(r'开始|张开|盛开', item['content']):
            return True
        else:
            for w in togo_ambiguous_words:
                if w in item['content']:
                    return False
            return True
    elif item['keyword'] == '滴滴共享汽车' or item['keyword'] == '小桔租车':
        if re.search(r'滴滴共享汽车|小桔', item['content']):
            return True
        else:
            return False
    else:
        return True


# 清洗提及数据
def cleanup_mention(mentions):
    mentions = list(filter(filter_offical_mention, mentions))
    mentions = list(filter(filter_irrelevant_mention, mentions))
    return mentions
    
    
# ———————————— 处理 ————————————

# 创建工作簿
wb = Workbook()
ws = wb.active
ws.title = '微博主号'

# 读取本月和上月的微博回采数据表，筛选出主号发文
primary_articles = list(filter(primary_filter, utils.get_articles(article_filename)))
primary_summary = summarize_primary(primary_articles)
last_primary_articles = list(filter(primary_filter, utils.get_articles(last_article_filename)))
last_primary_summary = summarize_primary(last_primary_articles)

# 分本月和上月的发文数和互动数，并计算环比涨幅
ws.append(['', label + '发文数', label + '互动数', last_label + '发文数', last_label + '互动数', '发文数环比', '互动数环比'])
for i, s in enumerate(primary_summary):
    ws.append([s['brand_name'], s['article_count'], s['interact_sum'],\
        last_primary_summary[i]['article_count'], last_primary_summary[i]['interact_sum'],\
        calc_relative_ratio_1(last_primary_summary[i]['article_count'] ,s['article_count']),\
        calc_relative_ratio_1(last_primary_summary[i]['interact_sum'] ,s['interact_sum'])])

# 发文情况
ct = arrow.now().shift(months=-1)
day_count = calendar.monthrange(ct.year, ct.month)[1]
days = list(map(lambda n: '{}/{}'.format(ct.month, n) , list(range(1, day_count + 1))))
for p in primarys:
    daily_pub = []
    pub_day_count = 0
    for d in range(day_count):
        daily_pub.append(len(list(filter(lambda item: item['id'] == p['id'] and item['publish_time'].day == d + 1, primary_articles))))
        pub_day_count += 1 if daily_pub[d] > 0 else 0
    p['daily_pub'] = daily_pub
    p['pub_day_count'] = pub_day_count

active_primarys = list(filter(lambda item: item['pub_day_count'], primarys))
ws.append([])
ws.append([''] +list(map(lambda item: item['brand_name'], active_primarys)))
for i, d in enumerate(days):
    ws.append([d] + list(map(lambda item: item['daily_pub'][i], active_primarys)))

# 发文天数
ws.append([])
ws.append(['', '发文天数'])
for p in primarys:
    ws.append([p['brand_name'], p['pub_day_count']])

# 粉丝增长数及增长率
ws.append([])
ws.append(['', last_label + '粉丝数', label + '粉丝数', '增长数量', '增长率'])
for p in primarys:
    ws.append([p['name'], p['last_follower_count'], p['follower_count'],p['follower_count'] - p['last_follower_count'],\
    calc_relative_ratio_1(p['last_follower_count'], p['follower_count'])])

# 粉丝男女占比
ws.append([])
last_followers = utils.get_followers(last_follower_filename)
ws.append(['', last_label + '女性占比', last_label + '男性占比'])
last_gender_percentages = calc_follower_gender_percentage(last_followers)
for p in last_gender_percentages:
    ws.append([p[0], p[1], p[2]])
        
ws.append([])
followers = utils.get_followers(follower_filename)
ws.append(['', label + '女性占比', label + '男性占比'])
gender_percentages = calc_follower_gender_percentage(followers)
for p in gender_percentages:
    ws.append([p[0], p[1], p[2]])


# 粉丝地域占比
ws.append([])
for p in primarys:
    if p['id'] in ['5992855888', '5698108446', '5620822338', '6323177781', '6545274324']:
        region_rank = calc_region_rank(p['id'], followers)
        last_region_rank = calc_region_rank(p['id'], last_followers)
        calc_region_rank_offset(region_rank, last_region_rank)

        ws.append(['', p['brand_name'], '地域排名变化'])
        for r in region_rank:
            ws.append([r['province'], str(r['percentage']) + '%', r['offset']])
        ws.append([])


# 获取本月及上月的提及数量，删除无关，计算增长数量和增长率
ws.append([])
mentions = utils.get_mentions(mention_filename)
last_metions = utils.get_mentions(last_mention_filename)
cleanup_mentions = cleanup_mention(mentions)

# 创建工作簿，保存清洗后的提及数据，人工进行复核
m_wb = Workbook()
m_ws = m_wb.active
m_ws.title = '微博提及'
m_ws.append(['提及关键词', '作者', '内容', '链接'])
for m in cleanup_mentions:
    m_ws.append([m['keyword'], m['author'], m['content'], m['url']])
m_wb.save(os.path.join(dist_path, '微博提及表_{}.xlsx'.format(label)))

print('微博提及数据已清洗，保存为 dist 目录下“微博提及表_{}.xlsx”，请人工复核'.format(label))
cleanup_flag = input('是否清洗干净（y/n）：')
if not cleanup_flag == 'y':
    print('退出脚本运行，若发现新歧义词则更新歧义词表后重新运行')
    exit()

for p in primarys:
    last_cleanup_mentions = cleanup_mention(last_metions)
    mention_summary = summarize_mention(p['keyword'], cleanup_mentions)
    last_mention_summary = summarize_mention(p['keyword'], last_cleanup_mentions)
    ws.append([])
    ws.append(['品牌', '上期数量', '本期数量', '增长数', '增长率'])
    mention_count_offset = mention_summary['mention_count'] - last_mention_summary['mention_count']
    mention_count_offset_percentage = calc_relative_ratio_1(last_mention_summary['mention_count'], mention_summary['mention_count'])
    ws.append([p['brand_name'], last_mention_summary['mention_count'], mention_summary['mention_count'],\
        mention_count_offset, mention_count_offset_percentage])
    
wb.save(os.path.join(dist_path, '微博主号统计表_{}.xlsx'.format(label)))
print('\n完成！结果文件保存在 dist 目录下')