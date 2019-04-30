#! /usr/bin/env python
# _*_ coding: utf-8 _*_

# 此脚本用于整理海量采集的视频数据，读取 src 目录下的 t_data_YYYYMM.xlsx，生成最终提交给客户的 1 个视频数据表

# 外部库（需要“pip install 库名”进行安装）
import arrow
from openpyxl import Workbook

# 内置库
import os

# 自定义模块
import utils
import fetchs

# 初始化
label = arrow.now().shift(months=-1).format('YYYYMM')
data_filename = 't_data_' + label + '.xlsx'
data_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src', data_filename)

# 读取 t_data 表，抽出视频数据
videos = utils.get_videos(data_filepath)
videos.sort(key=lambda video: video['brand'])

# 创建工作簿，写入视频数据，存到 dist 目录
wb = Workbook()
ws1 = wb.active
ws1.title = '视频'

ws1.append(['品牌','链接','id','发布时间','标题','视频平台','影响力'])
for video in videos:
    ws1.append([video['brand'], video['url'], video['id'], video['publish_time'], video['title'], video['platform'], video['impress']])

wb.save('./dist/视频数据表_{}.xlsx'.format(label))
print('\n完成！结果文件保存在 dist 目录下')

