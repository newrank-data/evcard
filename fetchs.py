#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 内置库
import re
import json

# 需要 pip 安装的外部库
import requests
from bs4 import BeautifulSoup

# 初始化
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'}
qqvideo_user_video_list_api = 'http://c.v.qq.com/vchannelinfo?otype=json&uin={}&qm=1&pagenum={}&num=30'
iqiyi_video_api = 'https://pcw-api.iqiyi.com/video/video/hotplaytimes/{}'
mgtv_video_api = 'https://pcweb.api.mgtv.com/episode/list?video_id={}&version=5.5.35'


def trans_num(str):
    if re.search(r'^\d+$', str):
        return int(str)
    elif '万' in str:
        f = float(str.split('万')[0])
        return int(f * 10000)
    elif '亿' in str:
        f = float(str.split('亿')[0])
        return int(f * 100000000)
    

def qqvideo(url):
    print('\nurl: ' + url)
    video_id_match = re.search(r'\/(\w{11})\.', url)
    video_id = None
    if not video_id_match:
        return {'video_id': None, 'play_num': -1}
    else:
        video_id = video_id_match.group(1)

    url = 'https://v.qq.com/x/page/{}.html'.format(video_id)
    video_res = requests.get(url, headers = headers)
    if video_res.status_code == 200:
        video_res.encoding = 'utf-8'
        video_soup = BeautifulSoup(video_res.text, 'lxml')
        play_num_text = video_soup.select('.action_count')[0].get_text()
        print('play_num_text: {}'.format(play_num_text.replace('\n', '')))
        video_play_num_match = re.search(r'([\w+|\.]+)次播放', play_num_text) 
        cover_play_num_match = re.search(r'([\w+|\.]+)次专辑播放', play_num_text) 
        
        if video_play_num_match:
            play_num = trans_num(video_play_num_match.group(1))
            return {'video_id': video_id, 'play_num': play_num}
        elif cover_play_num_match:
            user_page_url = video_soup.select('.user_info')[0].get('href')
            user_id = re.search(r'\w{32}', user_page_url).group(0)
            play_num = qqvideo_of_user(user_id, video_id)
            return {'video_id': video_id, 'play_num': play_num}
        else:
            return {'video_id': video_id, 'play_num': -1}
    else:
        return {'video_id': video_id, 'play_num': -1}


def qqvideo_of_user(user_id, target_video_id):
    has_more, match_flag, page, play_num = True, False, 1, 0
    while has_more and not match_flag:
        qqvideo_url = user_video_list_api.format(user_id, page)
        r = requests.get(url, headers = headers)
        
        if re.search(r'videolst":null', r.text):
            has_more = False
            break
        else:
            videos = json.loads(re.search(r'\[(.|\s)+\]', r.text).group(0))
            for video in videos:
                if video['vid'] == target_video_id:
                    play_num = int(trans_num(video['play_count']))
                    match_flag = True
                    break
        page += 1
                
    return play_num


def iqiyi(url):
    print('\nurl: ' + url)
    video_id_match = re.search(r'\_(\w{10})', url)
    video_id = None
    if video_id_match:
        video_id = video_id_match.group(1)
    else:
        return {'video_id': video_id, 'play_num': -1}
    
    video_res = requests.get(url, headers = headers)
    if video_res.status_code == 200:
        tv_id = re.search(r'param\[\'tvid\'\]\s=\s\"(\d+)', video_res.text).group(1)
        play_res = requests.get(iqiyi_video_api.format(tv_id), headers = headers)
        return {'video_id': video_id, 'play_num': json.loads(play_res.text)['data'][0]['hot']}
    else:
        {'video_id': video_id, 'play_num': -1}


def mgtv(url):
    print('\nurl: ' + url)
    video_id_match = re.search(r'(\d+)\.html', url)
    video_id = None
    if video_id_match:
        video_id = video_id_match.group(1)
    else:
        return {'video_id': video_id, 'play_num': -1}
    play_res = requests.get(mgtv_video_api.format(video_id), headers = headers)
    if play_res.status_code == 200:
        data = json.loads(play_res.text)['data']
        videos = data['list'] if data['list'] else data['short']
        play_num = 0
        for video in videos: 
            if video['video_id'] == video_id:
                play_num = trans_num(video['playcnt'])
                break
        return {'video_id': video_id, 'play_num': play_num}
    else:
        {'video_id': video_id, 'play_num': -1}
