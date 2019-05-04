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