#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/10/27 9:25
# @Author  : hui
# @Email   : huihuil@bupt.edu.cn
# @File    : newTasks.py
import json
import os
import pickle
from queue import Queue

from celery import task
from django.db import connection as conn
import re
from .models import disaster_info_cache as disaster_info_cache_table,disaster_info as disaster_info_table, parameter as parameter_table, post_extra as post_extra_table
import pandas as pd
import datetime

from .tasks import redisCache
from .util import earthquake_info
import redis
import pika
import pymongo
import logging
# 获得logger实例
logger = logging.getLogger('log')
import redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)

def load_obj(name):
    with open(os.path.dirname(os.path.abspath(__file__))+ '/' + 'resources/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

class Node:
    def __init__(self, ch):
        self.ch = ch
        self.root = False
        self.fail = None
        # 字符串的长度
        self.tail = 0
        self.child = {}
class ACTrie:
    def __init__(self):
        self.new_map = load_obj("location_map")
        self.root = Node("")
        self.root.root = True
        for k, v in self.new_map.items():
            self.insert(k)
        self.buildAC()

    def getLocationTest(self, s):
        ner_dict = self.run(s)
        return len(ner_dict) != 0
    def insert(self, strkey):
        p = self.root
        for s in strkey:
            if p.child.get(s, None):
                p = p.child.get(s)
            else:
                new_node = Node(s)
                p.child[s] = new_node
                p = new_node
        p.tail = len(strkey)
    def buildAC(self):
        q = Queue()
        q.put(self.root)
        while not q.empty():
            father = q.get()
            for c, child in father.child.items():
                if father.root:
                    child.fail = self.root
                else:
                    fail = father.fail
                    while fail:
                        if fail.child.get(c, None):
                            child.fail = fail.child.get(c)
                            break
                        fail = fail.fail
                    if not fail:
                        child.fail = self.root
                q.put(child)

    def run(self, ss):
        p = self.root
        res = {}
        idx = 0
        for s in ss:
            while not p.root and not p.child.get(s, None):
                p = p.fail
            if p.child.get(s, None):
                p = p.child.get(s)
            else:
                p = self.root
            tmp = p
            if not tmp.root and tmp.tail:
                res[idx - tmp.tail + 1] = idx
            idx += 1
        return res

    def getLocation(self, s, key, pre_dis, last_dis):
        ner_dict = self.run(s)
        ner_list = sorted(ner_dict.keys())
        for loc_idx in re.finditer(key, s):
            # 关键词开始位置
            key_loc_begin = int(loc_idx.span()[0])
            # 关键词结束位置
            key_loc_end = int(loc_idx.span()[1]) - 1
            for i in reversed(ner_list):
                if ner_dict[i] < key_loc_begin:
                    if abs(ner_dict[i] - key_loc_begin) <= pre_dis:
                        begin = i
                        end = ner_dict[i] + 1
                        loc = self.new_map.get(s[begin:end], None)
                        if loc is not None:
                            return True, loc
            for i in ner_list:
                if i > key_loc_end:
                    if abs(i - key_loc_end) < last_dis:
                        begin = i
                        end = ner_dict[i] + 1
                        loc = self.new_map.get(s[begin:end], None)
                        if loc is not None:
                            return True, loc
        return False, None


def earthquake_info(data):
    data = str(data)
    level = ''
    depth = ''
    if re.search("[0-9|\.]{1,4}级.{0,2}地震", str(data)) != None:
        level = re.search("[0-9|\.]{1,4}级.{0,2}地震", str(data)).group(0)
    if re.search("震源深度[0-9|\.|千米|公里|米|km|m]{1,5}", str(data)) != None:
        depth = re.search("震源深度[0-9|\.|千米|公里|米|km|m]{1,5}", str(data)).group(0)
    return level + ',' + depth if len(depth) != 0 else level


def isAuthentication(post, grade):
    try:
        post_content = post.get("post_content", "")
        del_words = "发生.{0,4}级|.{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|\(|\)|）|（|,|，|;|；|。|\?|？|！|!|:|：↓|【|】|\[|\]|'|#|<@>.*?</@>|<#>|</#>|http.*?</u>|<u>"
        post_content_tmp = re.sub(del_words, "", post_content)
        post_content_tmp = re.search("测定.*", post_content_tmp).group(0)
        flag, location = ner.getLocation(str(post_content_tmp), key_word, begin_dis, end_dis)
        if not flag:
            return False
        if "测定" in post.get("post_content", "") and post.get("authentication", "-100") == '1' and int(
                post.get("fans", 0)) > 100000 and float(grade) != -100:
            return True
    except:
        return False
    return False

# 超参数类型， 任务ID
task_id = "1"
now_time = datetime.datetime(2020, 1, 1, 0, 0)
end_time = datetime.datetime(2022, 1, 1, 0, 0)

# 读取数据事件差
days_data = 1
# 时间差，天
days_cha = 5
# 余震时间
days_cha_yuzhen = 1
# 同省市，但不包含等级，小时
days_cha_1 = 0.5
# 没有官方贴子的支撑下，事件监测的帖子阈值
days_cha_2 = 10
days_cha_3 = 8
days_cha_4 = 3


posts_a = 100
begin_dis = 12
end_dis = 10
key_word = '地震'
ner = ACTrie()


def eventDetect():
    logger.info("已经进入函数")
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    collection = db.posts
    sql = f'select * from disaster_info where task="{task_id}"'
    disaster_info = pd.read_sql(sql, conn)
    logger.info(f"共有{len(disaster_info)}次灾害事件")
    disaster_info_number = 0 if len(disaster_info) == 0 else disaster_info.number.max()
    # 查找
    sql = f'select * from disaster_info_cache where task = "{task_id}"'
    cache = pd.read_sql(sql, conn)
    logger.info(f"cache内数据量：{len(cache)}")
    posts = collection.find({"post_time": {"$gte": now_time, "$lt": end_time}, "task": task_id}).sort([("post_time", 1)])
    nums = collection.find({"post_time": {"$gte": now_time, "$lt": end_time}, "task": task_id}).count()
    logger.info(f"数据已经读取完毕,共有{nums}条待处理数据")
    # special_province = ["台湾省", "香港特别行政区", "澳门特别行政区"]
    del_words = "发生.{0,4}级|.{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|\(|\)|）|（|,|，|;|；|。|\?|？|！|!|:|：↓|【|】|\[|\]|'|#|<@>.*?</@>|<#>|</#>|http.*?</u>|<u>"

    Posts = db.Posts
    idx = 0
    for post in posts:
        idx += 1
        try:
            if idx % 1000 == 0:
                logger.info(idx)
            # 是否要继续
            conti = True
            post_time = post.get("post_time")
            post_id = post.get("post_id")
            media = post.get("media", "-100")
            post_content = post.get("post_content", "")
            location = None
            has_loc = False
            # 处理微博
            if post.get("media", None) == '1':
                # 如果转发了帖子，与其转发的帖子同类别
                repost_id = post.get("repost_id", None)
                if repost_id != None:
                    tmp = Posts.find({
                        "task": str(task_id),
                        "media": str(media),
                        "post_id": str(repost_id)
                    },{
                        "cluster": 1
                    })
                    for v in tmp:
                        post["cluster"] = int(v.get("cluster"))
                        try:
                            Posts.insert(post)
                        except:
                            pass
                        conti = False
                    if not conti:
                        continue
                # 查找包含的topic
                topics = re.findall("<#>(.*?)</#>", post_content)
                # 判断是否有相同HashTag
                # for i in topics:
                #     # 未完成
                #     pass
                # 先在hashtag中查找有效地点
                for i in topics:
                    # 在hashtag中查找有效地点
                    has_loc, location = ner.getLocation(str(i), key_word, begin_dis, end_dis)
                    if has_loc:
                        break
                # 去post_content查找有效地点
                if not has_loc:
                    post_content_tmp = re.sub(del_words, "", post_content)
                    has_loc, location = ner.getLocation(str(post_content_tmp), key_word, begin_dis, end_dis)
            # 处理新闻
            elif post.get("media", None) == '2':
                # 去题目中查找有效地点
                title = post.get("title", "")
                has_loc, location = ner.getLocation(str(title), title, begin_dis, end_dis)
                # 去内容中查找有效地点
                if not has_loc:
                    has_loc, location = ner.getLocation(str(post_content), key_word, begin_dis, end_dis)
                # print("新闻：", location)
            # 帖子中未提取到地点，利用发贴地代替
            if not has_loc:
                continue

            # 在帖子中提取出了地点
            province = location[0]
            city = location[1] if len(location[1]) > 0 else '-100'
            area = location[2] if len(location[2]) > 0 else '-100'

            # 提取灾害信息
            info = earthquake_info(post_content) if len(earthquake_info(post_content)) > 0 else '-100'
            grade = re.search("([0-9|\.]{1,4})", info).group(0) if re.search("([0-9|\.]{1,4})",
                                                                      info) != None and info != '-100' else '-100'
            if str(grade)[0] == '.':
                grade = -100
            elif float(grade) <= 0 or float(grade) > 10:
                grade = -100
            else:
                try:
                    grade = float(grade)
                except:
                    grade = -100
            # 如果是官方发布的帖子，省市时间差在一定范围内
            if isAuthentication(post, grade):
                post_content_tmp = re.sub(del_words, "", post_content)
                post_content_tmp = re.search("测定.*", post_content_tmp).group(0)
                _, location = ner.getLocation(str(post_content_tmp), key_word, begin_dis, end_dis)
                province = location[0]
                city = location[1] if len(location[1]) > 0 else '-100'
                area = location[2] if len(location[2]) > 0 else '-100'

                is_new_event = True

                is_jixu = True
                # 修正自动检测出的事件
                disaster_info_tmp = disaster_info[
                    (disaster_info.province == province) &
                    (disaster_info.time > post_time - datetime.timedelta(hours=6)) &
                    (disaster_info.authority == "0")
                    ]
                # 修正超过阈值检测出来的事件
                if len(disaster_info_tmp) != 0:
                    number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                    post["cluster"] = number
                    try:
                        Posts.insert(post)
                    except:
                        pass
                    # 修正自动检测出的灾害事件
                    disaster_info.loc[disaster_info.number == number, "info"] = info
                    disaster_info.loc[disaster_info.number == number, "city"] = city
                    disaster_info.loc[disaster_info.number == number, "area"] = area
                    disaster_info.loc[disaster_info.number == number, "grade"] = grade
                    disaster_info.loc[disaster_info.number == number, "authority"] = "1"
                    is_new_event = False
                    is_jixu = False

                # 同省市
                disaster_info_tmp = disaster_info[
                    (disaster_info.province == province) & (disaster_info.city == city) &
                    (disaster_info.time > post_time - datetime.timedelta(days=days_cha))
                    ]
                # 有该省市的地震记录了
                if len(disaster_info_tmp) != 0 and is_jixu:
                    grade_shaixuan = disaster_info_tmp[(disaster_info_tmp.grade == grade)]
                    # 找到了同等级的
                    if len(grade_shaixuan) != 0:
                        post["cluster"] = grade_shaixuan.iloc[len(grade_shaixuan) - 1].number
                        try:
                            Posts.insert(post)
                        except:
                            pass
                        is_new_event = False
                    else:
                        # 查找一天内发生过地震吗，判断余震
                        yuzhen_shaixuan = disaster_info_tmp[(disaster_info.time > post_time - datetime.timedelta(days=days_cha_yuzhen)) & (disaster_info_tmp.grade != -100)]
                        # 发生过地震了
                        if len(yuzhen_shaixuan) != 0:
                            number = grade_shaixuan.iloc[len(grade_shaixuan) - 1].number
                            post["cluster"] = number
                            try:
                                Posts.insert(post)
                            except:
                                pass
                            is_new_event = False
                        # 未发生地震
                        else:
                            # 查找事件表是否有地震事件了
                            grade_shaixuan = disaster_info_tmp[
                                (disaster_info_tmp.time > post_time - datetime.timedelta(days=days_cha_1))
                                ]
                            if len(grade_shaixuan) != 0:
                                number = grade_shaixuan.iloc[len(grade_shaixuan) - 1].number
                                post["cluster"] = number
                                try:
                                    Posts.insert(post)
                                except:
                                    pass
                                # 修正自动检测出的灾害事件
                                disaster_info.loc[disaster_info.number == number, "info"] = info
                                disaster_info.loc[disaster_info.number == number, "area"] = area
                                disaster_info.loc[disaster_info.number == number, "grade"] = grade
                                disaster_info.loc[disaster_info.number == number, "authority"] = "1"
                                is_new_event = False
                # 创建新事件
                if is_new_event:
                    disaster_info_number += 1
                    # 将提取到的灾害信息写入灾害信息表
                    disaster_info = disaster_info.append([{
                        "id": disaster_info_number,
                        "province": province,
                        "city": city,
                        "area": area,
                        "time": post_time,
                        "info": info,
                        "grade": grade,
                        "number": disaster_info_number,
                        "task": task_id,
                        "media": media,
                        "authority": '1',
                        "post_id": post_id
                    }])
                    # 前一小时内帖子写入
                    cache = cache.append([{
                        "province": province,
                        "city": city,
                        "area": area,
                        "post_time": post_time,
                        "info": info,
                        "task": task_id,
                        "media": media,
                        "post_id": post_id
                    }])
                    cache_tmp = cache[(cache.province == province) & (cache.post_time > post_time - datetime.timedelta(minutes=60))]
                    for cache_index, cache_post in cache_tmp.iterrows():
                        # 设置帖子的cluster字段
                        Posts.delete_one({
                            "post_id": cache_post.post_id,
                            "task": cache_post.task,
                            "media": cache_post.media,
                        })
                        for t in collection.find({
                            "post_id": cache_post.post_id,
                            "task": cache_post.task,
                            "media": cache_post.media,
                        }):
                            t["cluster"] = disaster_info_number
                            try:
                                Posts.insert(t)
                            except:
                                pass

                    # 清除cache中已经聚类的帖子
                    cache = cache.append(cache_tmp)
                    cache = cache.drop_duplicates(keep=False)
                    logger.info(f"官方帖子提取到了新灾害信息，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
            # 不是官方帖子
            else:
                # 判断省市县相似度
                p = province
                pc = None
                pca = None
                if city != "-100":
                    pc = province + city
                if area != "-100":
                    pca = province + city + area
                # 判断聚类是否结束
                is_cluster_end = False
                if pca is not None:
                    disaster_info_tmp = disaster_info[
                        (disaster_info.province + disaster_info.city + disaster_info.area == pca) &
                        (disaster_info.time > post_time - datetime.timedelta(days=days_cha_2))
                        ]
                    if len(disaster_info_tmp) != 0:
                        number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                        post["cluster"] = number
                        try:
                            Posts.insert(post)
                        except:
                            pass
                        is_cluster_end = True
                if not is_cluster_end and pc is not None:
                    disaster_info_tmp = disaster_info[
                        (disaster_info.province + disaster_info.area == pc) &
                        (disaster_info.time > post_time - datetime.timedelta(days=days_cha_3))
                        ]
                    if len(disaster_info_tmp) != 0:
                        number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                        post["cluster"] = number
                        try:
                            Posts.insert(post)
                        except:
                            pass
                        is_cluster_end = True
                if not is_cluster_end:
                    disaster_info_tmp = disaster_info[
                        (disaster_info.province == p) &
                        (disaster_info.time > post_time - datetime.timedelta(days=days_cha_4))
                        ]
                    if len(disaster_info_tmp) != 0:
                        number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                        post["cluster"] = number
                        try:
                            Posts.insert(post)
                        except:
                            pass
                        is_cluster_end = True

                if not is_cluster_end:
                    cache = cache.append([{
                        "province": province,
                        "city": city,
                        "area": area,
                        "post_time": post_time,
                        "info": info,
                        "task": task_id,
                        "media": media,
                        "post_id": post_id
                    }])
                    # 两小时内讨论此省地震信息的帖子数量大于一定阈值
                    cache_tmp = cache[(cache.province == province) & (cache.post_time > post_time - datetime.timedelta(hours=2))]
                    if len(cache_tmp) > posts_a:
                        disaster_info_number += 1
                        df_data_counts = pd.DataFrame(cache_tmp['city'].value_counts())
                        city = '-100'
                        for row in df_data_counts.iterrows():
                            if row[1].name != '-100' and int(row[1].city) > posts_a / 3:
                                city = str(row[1].name)
                            break
                        df_data_counts = pd.DataFrame(cache_tmp['area'].value_counts())
                        area = '-100'
                        for row in df_data_counts.iterrows():
                            if row[1].name != '-100' and int(row[1].area) > posts_a / 4:
                                area = row[1].name
                            break
                        disaster_info = disaster_info.append([{
                            "id": disaster_info_number,
                            "province": province,
                            "city": city,
                            "area": area,
                            "time": post_time,
                            "info": "可能发生了地震",
                            "grade": "-100",
                            "number": disaster_info_number,
                            "task": task,
                            "media": media,
                            "authority": '0',
                            "post_id": post_id
                        }])
                        # 将cache_tmp中已经聚类的帖子存入数据库
                        for cache_index, cache_post in cache_tmp.iterrows():
                            # 设置帖子的cluster字段
                            for t in collection.find({
                                "post_id": cache_post.post_id,
                                "task": cache_post.task,
                                "media": cache_post.media,
                            }):
                                t["cluster"] = disaster_info_number
                                try:
                                    Posts.insert(t)
                                except:
                                    pass
                        # 清除cache中已经聚类的帖子
                        cache = cache.append(cache_tmp)
                        cache = cache.drop_duplicates(keep=False)
                        logger.info(
                            f"社交媒体中自动检测出了新灾害记录，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
        except:
            print("*****except*****:", post.get("post_content"))
    # 写入数据
    logger.info(f"开始向disaster_info_cache表写入记录")
    disaster_info_cache_table.objects.all().delete()
    sql_create = []
    # 写入disaster_info_cache_table
    for index, record in cache.iterrows():
        if record.post_time > end_time - datetime.timedelta(days=days_data):
            t = disaster_info_cache_table(province=record.province, city=record.city, area=record.area,
                                          post_time=record.post_time, info=record.info,
                                          post_id=record.post_id, task=task_id, media=record.media)
            sql_create.append(t)
    disaster_info_cache_table.objects.bulk_create(sql_create, ignore_conflicts=True)

    # 写入disaster_info
    for index, record in disaster_info.iterrows():
        tmp = disaster_info_table.objects.filter(task=record.task, number=record.number)
        if tmp.exists():
            tmp.update(province=record.province, city=record.city, area=record.area,
                       time=record.time, info=record.info, number=record.number,
                       grade=record.grade, task=task_id, authority=record.authority, post_id=record.post_id)
            logger.info(f"修改disaster_info表")
        else:
            if float(record.grade) == -100 or (float(record.grade) > 0 and float(record.grade) < 10):
                disaster_info_table(province=record.province, city=record.city, area=record.area,
                                    time=record.time, info=record.info, number=record.number,
                                    grade=record.grade, task=task_id, authority=record.authority,
                                    post_id=record.post_id).save()
                logger.info(f"写入disaster_info表")
    sql = f'select * from disaster_info where task="{task_id}" and time > "{now_time - datetime.timedelta(days=days_data)}"'
    disaster_info = pd.read_sql(sql, conn)
    logger.info(f"共有{len(disaster_info)}次灾害事件")
    # 查找
    sql = f'select * from disaster_info_cache where task = "{task_id}"'
    cache = pd.read_sql(sql, conn)
    logger.info(f"cache内数据量：{len(cache)}")
    logger.info(f"一轮结束")





def getTestLocation():
    ner = ACTrie()
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    # 数据总量
    logger.info("数据读取开始")
    end_time = datetime.datetime(2020, 1, 1, 0, 0)
    posts = db.posts.find({
        "post_time": {"$gte": end_time},
        "task": "1",
        "media": "1"
    })
    logger.info("数据读取完毕")
    test_location = []
    idx = 0
    for post in posts:
        post_content = post.get("post_content", "")
        if ner.getLocationTest(post_content):
            test_location.append(post_content)
            idx += 1
            if idx % 1000 == 0:
                logger.info(idx)
            if idx == 500000:
                break
    logger.info("数据处理完毕：共:", len(test_location))
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + "test_location" + '.json', 'w', encoding='utf-8') as f:
        json.dump(test_location, f, ensure_ascii=False)
    logger.info("数据写入")
    return

