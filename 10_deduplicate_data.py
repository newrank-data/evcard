#! /usr/bin/env python
# _*_ coding: utf-8 _*_

# 此脚本用于对海量采集的数据进行去重，读取 src 目录下的 t_data_YYYYMM.xlsx，生成“重复数据表_YYYYMM.xlsx”

# 外部库（需要“pip install 库名”进行安装）
import arrow
from openpyxl import Workbook
from publicsuffixlist import PublicSuffixList


# 内置库
import os
import re

# 自定义模块
import utils
import fetchs

# 初始化
label = arrow.now().shift(months=-1).format('YYYYMM')
data_filename = 't_data_' + label + '.xlsx'
data_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src', data_filename)
psl = PublicSuffixList()
duplicate_domains = ['sina.com.cn', 'sina.cn', 'sohu.com', 'baidu.com', 'toutiao.com', 'qq.com',\
'ifeng.com', '163.com', 'eastday.com', 'eastmoney.com', 'people.com.cn', 'dzwww.com', '21cn.com',\
'chinanews.com', 'chinanews.com.cn', 'qihoo.com', 'bitauto.com', 'youth.cn', 'qctt.cn', 'tuxi.com.cn', 'autohome.com.cn']
rows = []


# ---------- 自定义函数 ----------

def extract_domain(item):
    hostname = None
    m = re.search(r'https?\:\/\/([\.|\w|\-]+)(\:\d+)?\/', item['url'])
    if m:
        hostname = m.group(1)
    else:
        print('🙃 oops~')
        print(item)
        exit()
    item['domain'] = psl.privatesuffix(hostname)
    item['subdomain'] = psl.privateparts(hostname)[0]
    return item


def deduplicate_sina(items):
    unique_ids = []
    for i in items:
        if i['subdomain'] == 'tousu':
            i['unique_id'] = re.search(r'\d{11}', i['url']).group(0)
        elif i['subdomain'] == 'blog':
            i['unique_id'] = re.search(r'[a-z0-9]{16,17}', i['url']).group(0)
        elif i['subdomain'] == 'guba':
            bid = re.search(r'bid=(\d+)', i['url']).group(1)
            tid = re.search(r'tid=(\d+)', i['url']).group(1)
            i['unique_id'] = bid + tid
        elif i['subdomain'] == 'vip' and 'id=' in i['url']:
            i['unique_id'] = re.search(r'id=(\d+)', i['url']).group(1)
        else:
            m = re.search(r'[a-z0-9]{14,18}', i['url'])
            if m:
                i['unique_id'] = m.group(0)
            else:
                i['unique_id'] = ''
                i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_toutiao(items):
    unique_ids = []
    for i in items:
        m = re.search(r'\d{16,19}', i['url'])
        if m:
            i['unique_id'] = m.group(0)
        else:
            i['unique_id'] = ''
            i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_sohu(items):
    unique_ids = []
    for i in items:
        if i['subdomain'] not in ['api', '3g']:
            m = re.search(r'\/(\d{9})\_', i['url'])
            if m:
                i['unique_id'] = m.group(1)
            elif '.shtml' in i['url']:
                i['unique_id'] = re.search(r'(\d{9})\.shtml', i['url']).group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''
        elif i['subdomain'] == '3g':
            i['unique_id'] = re.search(r'\/t\/n(\d+)', i['url']).group(1)
        elif i['subdomain'] == 'api':
            i['unique_id'] = re.search(r'newsId\=(\d+)', i['url']).group(1)

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_baidu(items):
    unique_ids = []
    items = list(filter(lambda i: i['source_type'] == '0' and not i['subdomain'] == 'gupiao', items))
    for i in items:
        if i['subdomain'] == 'cache':
            i['unique_id'] = re.search(r'\/c\?m=([a-z0-9]+)', i['url']).group(1)
        else:
            m = re.search(r'\d{17,19}', i['url'])
            if m:
                i['unique_id'] = m.group(0)
            else:
                i['unique_id'] = ''
                i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_qq(items):
    unique_ids = []
    items = list(filter(lambda i: i['subdomain'] not in ['v', 'coral'], items))
    items.sort(key=lambda i: i['subdomain'], reverse=True)
    for i in items:
        m = re.search(r'\d{8}[A-Z0-9]{6}', i['url'])
        if m:
            i['unique_id'] = m.group(0)
        elif 'com/a/' in i['url']:
            n = re.search(r'(\d{8})\/(\w{6})', i['url'])
            i['unique_id'] = n.group(1) + n.group(2)
        else:
            i['unique_id'] = ''
            i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_ifeng(items):
    unique_ids = []
    for i in items:
        if 'com/c/' in i['url']:
            i['unique_id'] = re.search(r'[\/|\_](\w{11})', i['url']).group(1)
        elif 'com/a/' in i['url']:
            i['unique_id'] = re.search(r'(\d+)\_0', i['url']).group(1)
        elif 'ucms_' in i['url']:
            i['unique_id'] = re.search(r'ucms\_(\w+)', i['url']).group(1)
        elif 'sub_' in i['url']:
            i['unique_id'] = re.search(r'sub\_(\w+)', i['url']).group(1)
        elif 'aid=' in i['url']:
            i['unique_id'] = re.search(r'aid\=(\d+)', i['url']).group(1)
        elif 'guid=' in i['url']:
            i['unique_id'] = re.search(r'guid\=([a-z0-9\-]+)', i['url']).group(1)
        elif '_0.shtml' in i['url']:
            i['unique_id'] = re.search(r'(\d+)\_0\.shtml', i['url']).group(1)
        elif '.shtml' in i['url']:
            i['unique_id'] = re.search(r'\/(\d+)\.shtml', i['url']).group(1)
        else:
            m = re.search(r'\d{8}\/(\d+)', i['url'])
            if m:
                i['unique_id'] = m.group(1)
            else:
                n = re.search(r'\d{4}\/\d{4}\/(\d+)', i['url'])
                if n:
                    i['unique_id'] = n.group(1)
                else:
                    i['unique_id'] = ''
                    i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_163(items):
    unique_ids = []
    items = list(items)
    items.sort(key=lambda i: i['subdomain'], reverse=True)
    for i in items:
        m = re.search(r'\w{16}', i['url'])
        if m:
            i['unique_id'] = m.group(0)
        else:
            i['unique_id'] = ''
            i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_eastday(items):
    unique_ids = []
    for i in items:
        m = re.search(r'\w{15}', i['url'])
        if m:
            i['unique_id'] = m.group(0)
        else:
            n = re.search(r'(\w+)\.html', i['url'])
            if n:
                i['unique_id'] = n.group(1)
            elif 'content_' in i['url']:
                i['unique_id'] = re.search(r'content\_(\d+)', i['url']).group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_eastmoney(items):
    unique_ids = []
    for i in items:
        if i['subdomain'] == 'caifuhao':
            m1 = re.search(r'\d+', i['url'])
            if m1:
                i['unique_id'] = m1.group(0)
            else:
                print('未匹配 unique_id', i)
                exit()
        elif i['subdomain'] == 'emwap':
            i['unique_id'] = re.search(r'\d{18}', i['url']).group(0)
        elif 'com/a/' in i['url']:
            i['unique_id'] = re.search(r'\d{18}', i['url']).group(0)
        else:
            m2 = re.search(r'(\d+)\.html', i['url'])
            if m2:
                i['unique_id'] = m2.group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''
        
        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_people(items):
    unique_ids = []
    items = filter(lambda i: not i['subdomain'] == 'liuyan', items)
    for i in items:
        m = re.search(r'(\d+)\.html', i['url'])
        if m:
            i['unique_id'] = m.group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_dzwww(items):
    unique_ids = []
    for i in items:
        m = re.search(r'(\d+)\.htm', i['url'])
        if m:
            i['unique_id'] = m.group(1)
        elif 'id=' in i['url']:
            i['unique_id'] = re.search(r'id\=(\d+)', i['url']).group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_21cn(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        if 'shtml' in i['url']:
            i['unique_id'] = str(i['release_date']) + i['title']
        elif i['subdomain'] == 'ts': 
            i['unique_id'] = re.search(r'id\/(\d+)', i['url']).group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_chinanews(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        m = re.search(r'([\d|\-]+)\.s?html', i['url'])
        if m:
            i['unique_id'] = m.group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_qihoo(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        m = re.search(r'[a-z0-9]{17}', i['url'])
        if m:
            i['unique_id'] = m.group(0)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_bitauto(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        if '.html' in i['url']:
            i['unique_id'] = re.search(r'(\d+)\.html', i['url']).group(1)
        else:
            m = re.search(r'wenzhang\/(\d+)', i['url'])
            if m:
                i['unique_id'] = m.group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_youth(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        if i['subdomain'] == 'kd':
            i['unique_id'] = re.search(r'signature\=(\w+)', i['url']).group(1)
        elif i['subdomain'] == 'kandian':
            i['unique_id'] = re.search(r'sign\=(\w+)', i['url']).group(1)
        else:
            m = re.search(r'\_(\d+)\.htm', i['url'])
            if m:
                i['unique_id'] = m.group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_qctt(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        m = re.search(r'\/([\_|\d]+)$', i['url'])
        if m:
            i['unique_id'] = m.group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_tuxi(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        m = re.search(r'(\w+)\.html', i['url'])
        if m:
            i['unique_id'] = m.group(1)
        else:
            i['unique_id'] = ''
            i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


def deduplicate_autohome(items):
    unique_ids = []
    extra_unique_ids = []
    for i in items:
        if i['subdomain'] == 'chejiahao':
            i['unique_id'] = re.search(r'\/(\d+)', i['url']).group(1)
        elif i['subdomain'] == 'forum':
            i['unique_id'] = re.search(r'\-t(\d{8})\-', i['url']).group(1)
        elif i['subdomain'] == 'club':
            m1 = re.search(r'(\d{8})\-\d+', i['url'])
            if m1:
                i['unique_id'] = m1.group(1)
            else:
                print('未匹配 unique_id', i)
                exit()
        elif i['subdomain'] == 'k':
            i['unique_id'] = re.search(r'\/(\w+)$', i['url']).group(1)
        elif i['subdomain'] == 'm':
            i['unique_id'] = re.search(r'\/(\d+)$', i['url']).group(1)
        else:
            m2 = re.search(r'(\d+)\.html', i['url'])
            if m2:
                i['unique_id'] = m2.group(1)
            else:
                i['unique_id'] = ''
                i['flag'] = ''

        if not i['unique_id'] == '':
            if i['unique_id'] in unique_ids:
                i['flag'] = 0
            else:
                unique_ids.append(i['unique_id'])
                i['flag'] = 1
        rows.append(i)


# --------- 处理过程 ----------


# 创建工作簿和工作表，等待写入数据
wb = Workbook()
ws = wb.active
ws.title = '去重数据表'
ws.append(['url_crc','url','souce_type','domain','subdomain','unique_id','flag'])


# 读取 t_data 表，过滤微博，提取主域名和子域名
print('读取 t_data 数据表，提取域名...')
datas = utils.get_datas(data_filepath)
datas = list(filter(lambda i: not i['source_type'] == '4', datas))
datas = list(map(extract_domain, datas))
datas.sort(key=lambda i: i['url'])
datas = list(filter(lambda i: i['domain'] in duplicate_domains, datas))


# 针对不同域名分别进行去重
deduplicate_sina(filter(lambda i: i['domain'] in ['sina.com.cn', 'sina.cn'], datas))
print('√ sina')
deduplicate_toutiao(filter(lambda i: i['domain'] == 'toutiao.com', datas))
print('√ toutiao')
deduplicate_sohu(filter(lambda i: i['domain'] == 'sohu.com', datas))
print('√ sohu')
deduplicate_baidu(filter(lambda i: i['domain'] == 'baidu.com', datas))
print('√ baidu')
deduplicate_qq(filter(lambda i: i['domain'] == 'qq.com', datas))
print('√ qq')
deduplicate_ifeng(filter(lambda i: i['domain'] == 'ifeng.com', datas))
print('√ ifeng')
deduplicate_163(filter(lambda i: i['domain'] == '163.com', datas))
print('√ 163')
deduplicate_eastday(filter(lambda i: i['domain'] == 'eastday.com', datas))
print('√ eastday')
deduplicate_eastmoney(filter(lambda i: i['domain'] == 'eastmoney.com', datas))
print('√ eastmoney')
deduplicate_people(filter(lambda i: i['domain'] == 'people.com.cn', datas))
print('√ people')
deduplicate_dzwww(filter(lambda i: i['domain'] == 'dzwww.com', datas))
print('√ dzwww')
deduplicate_21cn(filter(lambda i: i['domain'] == '21cn.com', datas))
print('√ 21cn')
deduplicate_chinanews(filter(lambda i: i['domain'] in ['chinanews.com.cn', 'chinanews.com'], datas))
print('√ chinanews')
deduplicate_qihoo(filter(lambda i: i['domain'] == 'qihoo.com', datas))
print('√ qihoo')
deduplicate_bitauto(filter(lambda i: i['domain'] == 'bitauto.com', datas))
print('√ bitauto')
deduplicate_youth(filter(lambda i: i['domain'] == 'youth.cn', datas))
print('√ youth')
deduplicate_qctt(filter(lambda i: i['domain'] == 'qctt.cn', datas))
print('√ qctt')
deduplicate_tuxi(filter(lambda i: i['domain'] == 'tuxi.com.cn', datas))
print('√ tuxi')
deduplicate_autohome(filter(lambda i: i['domain'] == 'autohome.com.cn', datas))
print('√ autohome')


for r in rows:
    ws.append([r['url_crc'], r['url'], r['source_type'], r['domain'], r['subdomain'], r['unique_id'], r['flag']])
wb.save('./dist/去重数据表_{}.xlsx'.format(label))
print('\n完成！结果文件保存在 dist 目录下')