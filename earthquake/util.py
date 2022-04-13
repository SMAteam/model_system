#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/10/6 15:53
# @Author  : hui
# @Email   : huihuil@bupt.edu.cn
# @File    : util.py
import pickle
import re
import datetime
import os

import ltp
from ltp import LTP

def save_obj(obj, name):
    with open(os.path.dirname(os.path.abspath(__file__))+ '/' + 'resources/' + name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
def load_obj(name):
    with open(os.path.dirname(os.path.abspath(__file__))+ '/' + 'resources/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)


P_not = load_obj("P")
C_not = load_obj("C")
A_not = load_obj("A")
RE = load_obj("RE")
RE_MAP = load_obj("RE_MAP")
Location_Alignment_Map = load_obj("RE_MAP")
DEL = ['东海','南海','河北','平安','南部','红旗','金山','海南']
def entityExtract(s, stop_words = [], P = True, C = True, A = True):
    res = []
    for i in re.finditer(RE, s):
        entity = i.group()
        if entity in stop_words:
            continue
        if not P:
            if entity in P_not:
                continue
        if not C:
            if entity in C_not:
                continue
        if not A:
            if entity in A_not:
                continue
        location = RE_MAP[entity]
        res.append({
                "entity": entity,
                "loc_begin": int(i.span()[0]),
                "loc_end": int(i.span()[1]),
                "location": location,
            })
    return res
# sentence 文本
# key_word 关键词
# begin_dis = 12 在关键词前多近范围内可提取地点
# end_dis = 8 在关键词后多近范围内可提取地点
# stop_words 地点停用词
# P = True 是否启用单个省份缩写检测
# C = True 是否启用单个市区缩写检测
# A = True 是否启用单个县区缩写检测
def disasterLocation(sentence, key_word, begin_dis = 12, end_dis = 8, stop_words = DEL, P = True, C = True, A = True, stop_others = ""):
    sentence = str(sentence)
    # 数据不符合规范
    if len(sentence) == 0 or sentence == None:
        return False
    sentence = re.sub(stop_others, "", sentence)
    entities = entityExtract(sentence, stop_words, P, C ,A)
    # 没有地点实体
    if (len(entities)) == 0:
        return False
    res = False
    # 搜索关键词位置
    for i in re.finditer(key_word, sentence):
        # 关键词开始位置
        key_loc_begin = int(i.span()[0])
        # 关键词结束位置
        key_loc_end = int(i.span()[1])
        # 返回值
        for entity in entities:
            # 寻找前方距离最近的
            if 0 <= key_loc_begin - entity['loc_end'] and key_loc_begin - entity['loc_end'] < begin_dis:
                res = entity
            if 0 <= entity['loc_begin'] - key_loc_end and entity['loc_begin'] - key_loc_end  < end_dis:
                if res != False:
                    return res
                res = entity
                return res
    return res
# 灾害信息获取,返回值
# data：帖子文本
def earthquake_info(data):
    data = str(data)
    level = ''
    depth = ''
    if re.search("[0-9|\.]{1,4}级.{0,2}地震", str(data)) != None:
        level = re.search("[0-9|\.]{1,4}级.{0,2}地震", str(data)).group(0)
    if re.search("震源深度[0-9|\.|千米|公里|米|km|m]{1,5}", str(data)) != None:
        depth = re.search("震源深度[0-9|\.|千米|公里|米|km|m]{1,5}", str(data)).group(0)
    return level + ',' + depth if len(depth) != 0 else level


class NerNeed:
    # def jaccard(a, b):
    #     a_count = {}
    #     b_count = {}
    #     for v in a:
    #         if a_count.get(v, None) != None:
    #             a_count[v] += 1
    #         else:
    #             a_count[v] = 1
    #     for v in b:
    #         if b_count.get(v, None) != None:
    #             b_count[v] += 1
    #         else:
    #             b_count[v] = 1
    #     sim_count = 0
    #     for k in a_count.keys():
    #         if b_count.get(k, None) != None:
    #             sim_count += min(a_count[k], b_count[k])
    #     return sim_count / (len(a) + len(b) - sim_count)
    #
    # def simOrganization(a, b):
    #     if a == b:
    #         return True
    #     if jaccard(a, b) >= 0.4:
    #         return True
    #     vec_sim = 0
    #     try:
    #         vec_sim = model.similarity(a, b)
    #         if vec_sim >= 0.85:
    #             return True
    #         else:
    #             return False
    #     except:
    #         return False
    #
    # def simPeople(a, b):
    #     if a == b:
    #         return True
    #     if jaccard(a, b) >= 0.5:
    #         return True
    #     vec_sim = 0
    #     try:
    #         vec_sim = model.similarity(a, b)
    #         if vec_sim >= 0.85:
    #             return True
    #         else:
    #             return False
    #     except:
    #         return False

    # s为字符串，key为关键词，pre_dis为前距离，last_dis为后距离
    def getLocation(self, s, key, pre_dis, last_dis):
        seg, hidden = ltp.seg([s])
        ner_words = ltp.ner(hidden)[0]
        # key为词的id,v为词的首部
        tmp_dict = {}
        i = 0
        now_idx = 0
        seg = seg[0]
        for v in seg:
            tmp_dict[i] = now_idx
            now_idx += len(v)
            i += 1
        # key为首部，value为尾部长度
        ner_dict = {}
        i = 0
        while i < len(ner_words):
            v = ner_words[i]
            tag, start, _ = v
            if tag != 'Ns':
                i += 1
                continue
            start_loc = tmp_dict[start]
            while i + 1 < len(ner_words) and ner_words[i + 1][0] == 'Ns' and ner_words[i + 1][1] == ner_words[i][2] + 1:
                i += 1
            end = ner_words[i][2]
            end_loc = tmp_dict[end] + len(seg[end])
            ner_dict[start_loc] = end_loc
            i += 1
        # 上边已经存储了地点的开始位置和结束位置
        ner_list = sorted(ner_dict.keys())
        for loc_idx in re.finditer(key, s):
            # 关键词开始位置
            key_loc_begin = int(loc_idx.span()[0])
            # 关键词结束位置
            key_loc_end = int(loc_idx.span()[1])
            first_words = None
            for i in reversed(ner_list):
                if ner_dict[i] <= key_loc_begin:
                    if abs(ner_dict[i] - key_loc_begin) <= pre_dis:
                        first_words = i
                        loc = Location_Alignment_Map.get(s[first_words:ner_dict[first_words]], None)
                        if loc != None:
                            return True, loc
            # 撒旦，国外名字
            second_words = None
            for i in ner_list:
                if i >= key_loc_end:
                    if abs(i - key_loc_end) <= last_dis:
                        second_words = i
                        loc = Location_Alignment_Map.get(s[second_words:ner_dict[second_words]], None)
                        if loc != None:
                            return True, loc
        return False, None

    # 提取有效机构实体
    def getOrganization(self, s, key, pre_dis, last_dis):
        seg, hidden = ltp.seg([s])
        ner_words = ltp.ner(hidden)[0]
        # key为词的id,v为词的首部
        tmp_dict = {}
        i = 0
        now_idx = 0
        seg = seg[0]
        for v in seg:
            tmp_dict[i] = now_idx
            now_idx += len(v)
            i += 1
        # key为首部，value为尾部长度
        ner_dict = {}
        i = 0
        while i < len(ner_words):
            v = ner_words[i]
            tag, start, _ = v
            if tag != 'Ns':
                i += 1
                continue
            start_loc = tmp_dict[start]
            while i + 1 < len(ner_words) and ner_words[i + 1][0] == 'Ns' and ner_words[i + 1][1] == ner_words[i][2] + 1:
                i += 1
            end = ner_words[i][2]
            end_loc = tmp_dict[end] + len(seg[end])
            ner_dict[start_loc] = end_loc
            i += 1

        # 上边已经存储了地点的开始位置和结束位置
        ner_list = sorted(ner_dict.keys())
        for loc_idx in re.finditer(key, s):
            # 关键词开始位置
            key_loc_begin = int(loc_idx.span()[0])
            # 关键词结束位置
            key_loc_end = int(loc_idx.span()[1])
            first_words = None
            for i in reversed(ner_list):
                if ner_dict[i] <= key_loc_begin:
                    if abs(ner_dict[i] - key_loc_begin) <= pre_dis:
                        first_words = i
                        loc = s[first_words:ner_dict[first_words]]
                        return True, loc
            second_words = None
            for i in ner_list:
                if i >= key_loc_end:
                    if abs(i - key_loc_end) <= last_dis:
                        second_words = i
                        loc = s[second_words:ner_dict[second_words]]
                        return True, loc

    # 提取有效人物实体
    def getPeople(self, s, key, pre_dis, last_dis):
        seg, hidden = ltp.seg([s])
        ner_words = ltp.ner(hidden)[0]
        # key为词的id,v为词的首部
        tmp_dict = {}
        i = 0
        now_idx = 0
        seg = seg[0]
        for v in seg:
            tmp_dict[i] = now_idx
            now_idx += len(v)
            i += 1
        # key为首部，value为尾部长度
        ner_dict = {}
        i = 0
        while i < len(ner_words):
            v = ner_words[i]
            tag, start, _ = v
            if tag != 'Ns':
                i += 1
                continue
            start_loc = tmp_dict[start]
            while i + 1 < len(ner_words) and ner_words[i + 1][0] == 'Ns' and ner_words[i + 1][1] == ner_words[i][2] + 1:
                i += 1
            end = ner_words[i][2]
            end_loc = tmp_dict[end] + len(seg[end])
            ner_dict[start_loc] = end_loc
            i += 1
        # 上边已经存储了地点的开始位置和结束位置
        ner_list = sorted(ner_dict.keys())
        for loc_idx in re.finditer(key, s):
            # 关键词开始位置
            key_loc_begin = int(loc_idx.span()[0])
            # 关键词结束位置
            key_loc_end = int(loc_idx.span()[1])
            first_words = None
            for i in reversed(ner_list):
                if ner_dict[i] <= key_loc_begin:
                    if abs(ner_dict[i] - key_loc_begin) <= pre_dis:
                        first_words = i
                        loc = s[first_words:ner_dict[first_words]]
                        return True, loc
            second_words = None
            for i in ner_list:
                if i >= key_loc_end:
                    if abs(i - key_loc_end) <= last_dis:
                        second_words = i
                        loc = s[second_words:ner_dict[second_words]]
                        return True, loc
        return False, None

ner = NerNeed()