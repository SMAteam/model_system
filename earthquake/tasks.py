#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/10/6 15:51
# @Author  : hui
# @Email   : huihuil@bupt.edu.cn
# @File    : tasks.py
import json
import os
import time

import hanlp
from celery import task
from django.db import connection as conn
import re

from ltp import LTP

from .models import event_time_line, disaster_info_cache as disaster_info_cache_table,disaster_info as disaster_info_table, parameter as parameter_table, post_extra as post_extra_table
import pandas as pd
import datetime

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from .util import disasterLocation,earthquake_info
import redis
import pika
import pymongo
import logging
# 获得logger实例
logger = logging.getLogger('log')
import redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + "停用词表", 'r', encoding='utf-8') as f:
    stop_words = f.read()
stop_words = stop_words.split('\n')
# 离线追赶数据
@task
def earthquakeDetect():
    logger.info("已经进入函数")
    now_time = datetime.datetime(2021,1,1,0,0)
    end_time = datetime.datetime(2021,3,28,0,0)
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    collection = db.posts  # 测试
    task = "1"
    days_cha = 15
    # 事件监测阈值
    posts_a = 100
    begin_dis = 12
    end_dis = 10
    sql = f'select * from disaster_info where task="{task}" and time > "{now_time - datetime.timedelta(days=days_cha)}"'
    disaster_info = pd.read_sql(sql, conn)
    disaster_info_number = 1 if len(disaster_info) == 0 else disaster_info.number.max() + 1
    columns = ['post_id', 'task', 'media', 'cluster']
    post_extra = pd.DataFrame(columns=columns)
    sql = f'select * from disaster_info_cache where task = "{task}"'
    cache = pd.read_sql(sql, conn)
    posts = collection.find({"post_time": {"$gte": now_time, "$lt": end_time}}).sort([("post_time",1)])
    nums = collection.find({"post_time": {"$gte": now_time, "$lt": end_time}}).count()
    logger.info(f"数据已经读取完毕,共有{nums}条待处理数据")
    special_province = ["台湾省", "香港特别行政区", "澳门特别行政区"]
    key_word = '地震'
    stop_others = ".{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|\(|\)|）|（|,|，|;|；|。|\?|？|！|!|:|：↓|【|】|<@>.*?</@>"
    Posts = db.Posts
    for post in posts:
        conti = True
        post_time = post.get("post_time")
        post_id = post.get("post_id")
        media = post.get("media")
        post_content = post.get("post_content")
        location = None
        # 如果转发了帖子，与其转发的帖子同类别
        if post.get("media", None) == '1':
            if post.get("repost_id", None) != None:
                # logger.info("转发帖子")
                tmp = Posts.find({
                    "task": str(task),
                    "media": str(media),
                    "post_id": str(post.get("repost_id"))
                },{
                    "cluster": 1
                })
                for v in tmp:
                    post_extra = post_extra.append([{
                        "post_id": post_id,
                        "task": task,
                        "media": media,
                        "cluster": int(v.get("cluster"))
                    }])
                    conti = False
                if not conti:
                    continue
            topics = re.findall("<#>(.*?)</#>", post_content)
            for i in topics:
                location = disasterLocation(i, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                            end_dis=end_dis)
                if location != False and (
                        len(location['location'][1]) > 0 or location['location'][0] in special_province):
                    break
                location = False
            if location == None or location == False:
                location = disasterLocation(post_content, key_word, stop_others=stop_others + "|(<#>.*?</#>)",
                                            begin_dis=begin_dis, end_dis=end_dis)
            if location == None or location == False:
                for i in topics:
                    location = disasterLocation(i, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                                end_dis=end_dis)
                    if location != False:
                        break
        elif post.get("media", None) == '2':
            title = post.get("title", None)
            location = disasterLocation(title, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                        end_dis=end_dis)
            if location == None or location == False:
                location = disasterLocation(post_content, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                            end_dis=end_dis)
        # 提取灾害信息
        info = earthquake_info(post_content) if len(earthquake_info(post_content)) > 0 else '-100'
        # 提取地点信息

        # 在帖子中未提取出地点
        province = '-100'
        city = '-100'
        area = '-100'
        grade = '-100'
        # 帖子中未提取到地点，利用发贴地代替
        if location == None or location == False:
            continue
        # 在帖子中提取出了地点
        else:
            province = location['location'][0]
            city = location['location'][1] if len(location['location'][1]) > 0 else '-100'
            area = location['location'][2] if len(location['location'][2]) > 0 else '-100'

        grade = re.search("([0-9|\.]{1,4})", info).group(0) if re.search("([0-9|\.]{1,4})",
                                                                         info) != None and info != '-100' else '-100'
        # 如果是官方发布的帖子
        if grade != "-100" and post.get("authentication", None) != None and int(post.get("fans",
                                                                                         None)) != None and post.get(
            "authentication") == '1' and ("测定" in post_content or "地震台网正式测定" in post_content) and len(
            post_content) < 150 and int(post.get("fans")) > 100000:
            disaster_info_tmp = disaster_info[
                (disaster_info.province == province) & (disaster_info.city == city) & (
                        disaster_info.time > post_time - datetime.timedelta(days=days_cha)) & (
                        (disaster_info.grade == grade) | (disaster_info.grade == "-100"))]
            # 如果没有该省市地震信息，直接创建
            if len(disaster_info_tmp) == 0:
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
                    "task": task,
                    "media": media,
                    "authority": '1',
                    "post_id": post_id
                }])
                # 前十五分钟内帖子写入
                cache = cache.append([{
                    "province": province,
                    "city": city,
                    "area": area,
                    "post_time": post_time,
                    "info": info,
                    "task": task,
                    "media": media,
                    "post_id": post_id
                }])
                cache_tmp = cache[
                    (cache.province == province) & (cache.post_time > post_time - datetime.timedelta(minutes=15))]
                for cache_index, cache_post in cache_tmp.iterrows():
                    # 设置帖子的cluster字段
                    post_extra = post_extra.append([{
                        "post_id": cache_post.post_id,
                        "task": cache_post.task_id,
                        "media": cache_post.media,
                        "cluster": disaster_info_number
                    }])

                # 清除cache中已经聚类的帖子
                cache = cache.append(cache_tmp)
                cache = cache.drop_duplicates(keep=False)
                disaster_info_number = disaster_info_number + 1
                logger.info(f"官方帖子提取到了新灾害信息，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
                conti = False
            if not conti:
                continue
            # 如果有该省市地震信息
            authority = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].authority
            # 进行信息修正
            if authority == "0":
                number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                disaster_info.loc[disaster_info.number == number, "info"] = info
                disaster_info.loc[disaster_info.number == number, "area"] = area
                disaster_info.loc[disaster_info.number == number, "grade"] = grade
                disaster_info.loc[disaster_info.number == number, "authority"] = "1"
                logger.info(f"官方帖子修正了灾害信息，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")

            post_extra = post_extra.append([{
                "post_id": post_id,
                "task": task,
                "media": media,
                "cluster": disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
            }])
            conti = False
        if not conti:
            continue
        # 不是官方帖子或者没有提取到grade
        # 有市信息
        if city != '-100':
            disaster_tmp = disaster_info[(disaster_info.province == province) & (disaster_info.city == city) & (
                    disaster_info.time > post_time - datetime.timedelta(days=days_cha))]
            # 在灾害信息表中找到了符合条件的灾害
            if len(disaster_tmp) != 0:
                post_extra = post_extra.append([{
                    "post_id": post_id,
                    "task": task,
                    "media": media,
                    "cluster": disaster_tmp.iloc[len(disaster_tmp) - 1].number
                }])
                continue
        # 没有市信息
        disaster_tmp = disaster_info[(disaster_info.province == province) & (
                disaster_info.time > post_time - datetime.timedelta(days=days_cha))]
        # 在灾害信息表中找到了符合条件的灾害，直接归到省级
        if len(disaster_tmp) != 0:
            post_extra = post_extra.append([{
                "post_id": post_id,
                "task": task,
                "media": media,
                # BUG
                "cluster": disaster_tmp.iloc[len(disaster_tmp) - 1].number
            }])
            continue
        # 有可能发生新地震了
        cache = cache.append([{
            "province": province,
            "city": city,
            "area": area,
            "post_time": post_time,
            "info": info,
            "task": task,
            "media": media,
            "post_id": post_id
        }])
        cache_tmp = cache[
            (cache.province == province) & (cache.post_time > post_time - datetime.timedelta(hours=2))]
        # 非直辖市帖子阈值达标
        if len(cache_tmp) > posts_a:  # and province not in special_province
            df_data_counts = pd.DataFrame(cache_tmp['city'].value_counts())
            city = '-100'
            for row in df_data_counts.iterrows():
                if row[1].name != '-100' and int(row[1].city) > posts_a / 4:
                    city = row[1].name
                    break

            df_data_counts = pd.DataFrame(cache_tmp['area'].value_counts())
            area = '-100'
            for row in df_data_counts.iterrows():
                if row[1].name != '-100' and int(row[1].area) > posts_a / 8:
                    area = row[1].name
                    break
            if city != "-100":
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
                    post_extra = post_extra.append([{
                        "post_id": cache_post.post_id,
                        "task": cache_post.task_id,
                        "media": cache_post.media,
                        "cluster": disaster_info_number
                    }])
                disaster_info_number = disaster_info_number + 1
                cache = cache.append(cache_tmp)
                cache = cache.drop_duplicates(keep=False)
                logger.info(f"社交媒体中自动检测出了新灾害记录，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
    logger.info(f"开始向Posts表写入记录")
    # 写入post_extra
    for index, record in post_extra.iterrows():
        data = collection.find({"task": record.task, "post_id": record.post_id, "media": record.media})
        for i in data:
            i["cluster"] = record.cluster
            try:
                Posts.insert(i)
            except:
                pass
    logger.info(f"开始向disaster_info_cache表写入记录")
    disaster_info_cache_table.objects.all().delete()
    sql_create = []
    # 写入disaster_info_cache_table
    for index, record in cache.iterrows():
        if record.post_time > end_time - datetime.timedelta(days=days_cha):
            t = disaster_info_cache_table(province=record.province, city=record.city, area=record.area,
                                          post_time=record.post_time, info=record.info,
                                          post_id=record.post_id, task=record.task, media=record.media)
            sql_create.append(t)
    disaster_info_cache_table.objects.bulk_create(sql_create, ignore_conflicts = True)
    logger.info(f"开始向disaster_info表写入记录")
    # 写入disaster_info
    for index, record in disaster_info.iterrows():
        tmp = disaster_info_table.objects.filter(task=record.task, number=record.number)
        if tmp.exists():
            tmp.update(province=record.province, city=record.city, area=record.area,
                       time=record.time, info=record.info, number=record.number,
                       grade=record.grade, task=record.task, authority=record.authority, post_id=record.post_id)
            logger.info(f"修改disaster_info表")
        else:
            if float(record.grade) == -100 or (float(record.grade) > 0 and float(record.grade) < 10):
                disaster_info_table(province=record.province, city=record.city, area=record.area,
                                    time=record.time, info=record.info, number=record.number,
                                    grade=record.grade, task=record.task, authority=record.authority,
                                    post_id=record.post_id).save()
                logger.info(f"写入disaster_info表")
    logger.info(f"一轮结束")
    redisCache()
    return
@task
def redisCache():
    logger.info("Redis缓存开始")
    task = "1"
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    redisConn = redis.Redis(connection_pool=pool)

    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    pipeline = [
        {"$match": {"task": task, "media": "1"}},
        {"$group": {"_id": "$province", "count": {"$sum": 1}}}
    ]
    records = db.Posts.aggregate(pipeline)
    res = []
    for row in records:
        if row.get("_id") != "海外" and row.get("_id") != "-100":
            res.append({
                "name": row.get("_id"),
                "value": int(row.get("count"))
            })
    redisConn.set("earthquake_macroscopic_map2", res)
    logger.info("Redis缓存1结束")
    records = db.Posts.aggregate([{
        "$match": {
            "task": task
        }
    }, {
        "$project": {
            "post_time": {
                "$dateToString": {
                    "format": "%Y-%m",
                    "date": "$post_time"
                }
            },
        }
    }, {
        "$group": {
            "_id": "$post_time",
            "count": {
                "$sum": 1
            }
        }
    }, {
        "$project": {
            "name": {
                "$dateFromString": {
                    "dateString": "$_id",
                }
            },
            "value": {
                "$abs": "$count"
            },
        }
    }, {
        "$sort": {
            "_id": 1
        }
    }
    ], allowDiskUse=True)
    res = []
    for row in records:
        del row["_id"]
        row["name"] = row["name"].strftime('%Y-%m')
        res.append(row)
    redisConn.set("earthquake_macroscopic_bar1", res)
    logger.info("Redis缓存2结束")
    res = {}
    countTotal = db.posts.find({
        "task": task,
    }).count()
    countEvent = disaster_info_table.objects.filter(task=task).count()
    res['countTotal'] = countTotal
    res['countEvent'] = countEvent
    redisConn.set("earthquake_macroscopic_basestatistics", res)
    logger.info("Redis缓存完毕")
    return

@task
def addHot():
    logger.info("开始计算热度")
    task = "1"
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    records = disaster_info_table.objects.filter(task=task)
    for record in records:
        number = record.number
        count = db.Posts.find({
            "task": task,
            "cluster": number
        }).count()
        tmp = disaster_info_table.objects.filter(task=task, number=number)
        if tmp.exists():
            tmp.update(hot=count)
            logger.info(f"修改disaster_info表编号{number},热度 {count}")
    return

@task
def earthquakeDetectQueue():
    logger.info("消费者程序启动")
    credentials = pika.PlainCredentials('root', 'buptweb007')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, virtual_host='scrapy', credentials=credentials))
    channel = connection.channel()
    # 申明消息队列，消息在这个队列传递，如果不存在，则创建队列
    channel.queue_declare(queue='earthquake', durable=False)

    def earthquakeDetectConsumer(ch, method, properties, body):
        logger.info("已经进入处理函数")
        now_time = datetime.datetime.now()
        # 事件讨论延续时长
        cursor = conn.cursor()
        client = pymongo.MongoClient("localhost", 27017)
        db = client.admin
        db.authenticate('root', 'buptweb007')
        db = client.SocialMedia
        collection = db.posts  # 测试
        task = "1"
        days_cha = 15
        # 事件监测阈值
        posts_a = 100
        begin_dis = 12
        end_dis = 10
        sql = f'select * from disaster_info where task="{task}" and time > "{now_time - datetime.timedelta(days=days_cha)}"'
        disaster_info = pd.read_sql(sql, conn)
        disaster_info_number = 1 if len(disaster_info) == 0 else disaster_info.number.max() + 1
        columns = ['post_id', 'task', 'media', 'cluster']
        post_extra = pd.DataFrame(columns=columns)
        sql = f'select * from disaster_info_cache where task = "{task}"'
        cache = pd.read_sql(sql, conn)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        posts = json.loads(body)
        logger.info(f"数据已经读取完毕,共有{len(posts)}条待处理数据")
        special_province = ["台湾省", "香港特别行政区", "澳门特别行政区"]
        key_word = '地震'
        stop_others = ".{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|\(|\)|）|（|,|，|;|；|。|\?|？|！|!|:|：↓|【|】|<@>.*?</@>"
        Posts = db.Posts
        for post in posts:
            post_time = datetime.datetime.strptime(post.get("post_time"), "%Y-%m-%d %H:%M:%S")
            post_id = post.get("post_id")
            media = post.get("media")
            post_content = post.get("post_content")
            location = None
            # 如果转发了帖子，与其转发的帖子同类别
            if post.get("media", None) == '1':
                if post.get("repost_id", None) != None:
                    # logger.info("转发帖子")
                    tmp = Posts.find({
                        "task": str(task),
                        "media": str(media),
                        "post_id": str(post.get("repost_id"))
                    }, {
                        "cluster": 1
                    })
                    for v in tmp:
                        post_extra = post_extra.append([{
                            "post_id": post_id,
                            "task": task,
                            "media": media,
                            "cluster": int(v.get("cluster"))
                        }])
                        continue
                topics = re.findall("<#>(.*?)</#>", post_content)
                for i in topics:
                    location = disasterLocation(i, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                                end_dis=end_dis)
                    if location != False and (
                            len(location['location'][1]) > 0 or location['location'][0] in special_province):
                        break
                    location = False
                if location == None or location == False:
                    location = disasterLocation(post_content, key_word, stop_others=stop_others + "|(<#>.*?</#>)",
                                                begin_dis=begin_dis, end_dis=end_dis)
                if location == None or location == False:
                    for i in topics:
                        location = disasterLocation(i, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                                    end_dis=end_dis)
                        if location != False:
                            break
            elif post.get("media", None) == '2':
                title = post.get("title", None)
                location = disasterLocation(title, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                            end_dis=end_dis)
                if location == None or location == False:
                    location = disasterLocation(post_content, key_word, stop_others=stop_others, begin_dis=begin_dis,
                                                end_dis=end_dis)
            # 提取灾害信息
            info = earthquake_info(post_content) if len(earthquake_info(post_content)) > 0 else '-100'
            # 提取地点信息

            # 在帖子中未提取出地点
            province = '-100'
            city = '-100'
            area = '-100'
            grade = '-100'
            # 帖子中未提取到地点，利用发贴地代替
            if location == None or location == False:
                continue
            # 在帖子中提取出了地点
            else:
                province = location['location'][0]
                city = location['location'][1] if len(location['location'][1]) > 0 else '-100'
                area = location['location'][2] if len(location['location'][2]) > 0 else '-100'

            grade = re.search("([0-9|\.]{1,4})", info).group(0) if re.search("([0-9|\.]{1,4})",
                                                                             info) != None and info != '-100' else '-100'
            # 如果是官方发布的帖子
            # print(post_content)
            if grade != "-100" and post.get("authentication", None) != None and int(post.get("fans",
                                                                                             None)) != None and post.get(
                "authentication") == '1' and ("测定" in post_content or "地震台网正式测定" in post_content) and len(
                post_content) < 150 and int(post.get("fans")) > 100000:
                disaster_info_tmp = disaster_info[
                    (disaster_info.province == province) & (disaster_info.city == city) & (
                            disaster_info.time > post_time - datetime.timedelta(days=days_cha)) & (
                            (disaster_info.grade == grade) | (disaster_info.grade == "-100"))]
                # 如果没有该省市地震信息，直接创建
                if len(disaster_info_tmp) == 0:
                    # 将提取到的灾害信息写入灾害信息表
                    disaster_info = disaster_info.append([{
                        "province": province,
                        "city": city,
                        "area": area,
                        "time": post_time,
                        "info": info,
                        "grade": grade,
                        "number": disaster_info_number,
                        "task": task,
                        "media": media,
                        "authority": '1',
                        "post_id": post_id
                    }])
                    # 前十五分钟内帖子写入
                    cache = cache.append([{
                        "province": province,
                        "city": city,
                        "area": area,
                        "post_time": post_time,
                        "info": info,
                        "task": task,
                        "media": media,
                        "post_id": post_id
                    }])
                    cache_tmp = cache[
                        (cache.province == province) & (cache.post_time > post_time - datetime.timedelta(minutes=15))]
                    for cache_index, cache_post in cache_tmp.iterrows():
                        # 设置帖子的cluster字段
                        post_extra = post_extra.append([{
                            "post_id": cache_post.post_id,
                            "task": cache_post.task_id,
                            "media": cache_post.media,
                            "cluster": disaster_info_number
                        }])

                    # 清除cache中已经聚类的帖子
                    cache = cache.append(cache_tmp)
                    cache = cache.drop_duplicates(keep=False)
                    disaster_info_number += 1
                    logger.info(f"官方帖子提取到了新灾害信息，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}，帖子id为")
                    continue
                # 如果有该省市地震信息
                authority = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].authority
                # 进行信息修正
                if authority == "0":
                    number = disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                    disaster_info.loc[disaster_info.number == number, "info"] = info
                    disaster_info.loc[disaster_info.number == number, "area"] = area
                    disaster_info.loc[disaster_info.number == number, "grade"] = grade
                    disaster_info.loc[disaster_info.number == number, "authority"] = "1"
                    logger.info(f"官方帖子修正了灾害信息，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
                post_extra = post_extra.append([{
                    "post_id": post_id,
                    "task": task,
                    "media": media,
                    "cluster": disaster_info_tmp.iloc[len(disaster_info_tmp) - 1].number
                }])
                continue
            # 不是官方帖子或者没有提取到grade
            # 有市信息
            if city != '-100':
                disaster_tmp = disaster_info[(disaster_info.province == province) & (disaster_info.city == city) & (
                        disaster_info.time > post_time - datetime.timedelta(days=days_cha))]
                # 在灾害信息表中找到了符合条件的灾害
                if len(disaster_tmp) != 0:
                    post_extra = post_extra.append([{
                        "post_id": post_id,
                        "task": task,
                        "media": media,
                        "cluster": disaster_tmp.iloc[len(disaster_tmp) - 1].number
                    }])
                    continue
            # 没有市信息
            disaster_tmp = disaster_info[(disaster_info.province == province) & (
                    disaster_info.time > post_time - datetime.timedelta(days=days_cha))]
            # 在灾害信息表中找到了符合条件的灾害，直接归到省级
            if len(disaster_tmp) != 0:
                post_extra = post_extra.append([{
                    "post_id": post_id,
                    "task": task,
                    "media": media,
                    "cluster": disaster_tmp.iloc[len(disaster_tmp) - 1].number
                }])
                continue
            # 有可能发生新地震了
            cache = cache.append([{
                "province": province,
                "city": city,
                "area": area,
                "post_time": post_time,
                "info": info,
                "task": task,
                "media": media,
                "post_id": post_id
            }])
            cache_tmp = cache[
                (cache.province == province) & (cache.post_time > post_time - datetime.timedelta(hours=2))]
            # 非直辖市帖子阈值达标
            if len(cache_tmp) > posts_a:  # and province not in special_province
                df_data_counts = pd.DataFrame(cache_tmp['city'].value_counts())
                city = '-100'
                for row in df_data_counts.iterrows():
                    if row[1].name != '-100' and int(row[1].city) > posts_a / 4:
                        city = row[1].name
                        break

                df_data_counts = pd.DataFrame(cache_tmp['area'].value_counts())
                area = '-100'
                for row in df_data_counts.iterrows():
                    if row[1].name != '-100' and int(row[1].area) > posts_a / 8:
                        area = row[1].name
                        break
                if city != "-100":
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
                        post_extra = post_extra.append([{
                            "post_id": cache_post.post_id,
                            "task": cache_post.task_id,
                            "media": cache_post.media,
                            "cluster": disaster_info_number
                        }])
                    disaster_info_number = disaster_info_number + 1
                    cache = cache.append(cache_tmp)
                    cache = cache.drop_duplicates(keep=False)
                    logger.info(
                        f"社交媒体中自动检测出了新灾害记录，帖子内容为{post_content}，地点为：{province}{city}{area}，等级为{grade}，详情为：{info}")
        logger.info(f"开始向Posts表写入记录")
        # 写入post_extra
        Posts_cache = []
        for index, record in post_extra.iterrows():
            data = collection.find({"task": record.task, "post_id": record.post_id, "media": record.media})
            for i in data:
                i["cluster"] = record.cluster
                try:
                    Posts.insert(i)
                except:
                    pass
        logger.info(f"开始向disaster_info_cache表写入记录")
        disaster_info_cache_table.objects.all().delete()
        sql_create = []
        # 写入disaster_info_cache_table
        for index, record in cache.iterrows():
            if record.post_time > now_time - datetime.timedelta(days=days_cha):
                t = disaster_info_cache_table(province=record.province, city=record.city, area=record.area,
                                              post_time=record.post_time, info=record.info,
                                              post_id=record.post_id, task=record.task, media=record.media)
                sql_create.append(t)
        disaster_info_cache_table.objects.bulk_create(sql_create, ignore_conflicts=True)
        logger.info(f"开始向disaster_info表写入记录")
        # 写入disaster_info
        for index, record in disaster_info.iterrows():
            tmp = disaster_info_table.objects.filter(task=record.task, number=record.number)
            if tmp.exists():
                tmp.update(province=record.province, city=record.city, area=record.area,
                           time=record.time, info=record.info, number=record.number,
                           grade=record.grade, task=record.task, authority=record.authority, post_id=record.post_id)
                logger.info(f"修改disaster_info表")
            else:
                if float(record.grade) == -100 or (float(record.grade) > 0 and float(record.grade) < 10):
                    disaster_info_table(province=record.province, city=record.city, area=record.area,
                                        time=record.time, info=record.info, number=record.number,
                                        grade=record.grade, task=record.task, authority=record.authority,
                                        post_id=record.post_id).save()
                    logger.info(f"写入disaster_info表")
        redisCache()
        logger.info(f"一轮结束")
        return
    # 告诉rabbitmq，用callback来接收消息
    channel.basic_consume('earthquake', earthquakeDetectConsumer)
    # 开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
    channel.start_consuming()
    return
@task
def weibo():
    print("begin")
    cursor = conn.cursor()
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root','buptweb007')
    db = client.SocialMedia
    collection = db.posts
    print('begin1')
    sql = f'select a.user_id,post_id,post_content,post_time,forward_num,comment_num,like_num,repost_id,user_name,province,city,authentication,fans,interest,weibo_num from weibo_post a join weibo_user b on a.user_id=b.user_id and post_time >= "2020-01-01" and post_time < "2020-11-10"'
    cursor.execute(sql)
    data = cursor.fetchall()
    print('begin2')
    for idx,v in enumerate(data):
        if idx % 1000 == 0:
            print(idx)
        collection.insert({
            "user_id": v[0],
            "post_id": v[1],
            "post_content": v[2],
            "post_time": v[3],
            "forward_num": v[4],
            "comment_num": v[5],
            "like_num": v[6],
            "repost_id": v[7],
            "user_name": v[8],
            "province": v[9],
            "city": v[10],
            "authentication": v[11],
            "fans": v[12],
            "interest": v[13],
            "weibo_num": v[14],
            "media": "1",
            "task": "1",
            "post_url": "https://weibo.com/" + str(v[0]) + "/" + str(v[1]) + "/",
        })
    print("end")
    return
@task
def xinwen():
    cursor = conn.cursor()
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root','buptweb007')
    db = client.SocialMedia
    collection = db.posts
    print('begin1')
    sql = f'select user_name,post_id,post_content,post_time,title,brief,detail_link from xinlang_new where  post_time >= "2020-01-01" and post_time < "2020-11-10"'
    cursor.execute(sql)
    data = cursor.fetchall()
    print('begin2')
    for idx,v in enumerate(data):
        if idx % 1000 == 0:
            print(idx)
        collection.insert({
            "user_name": v[0],
            "post_id": v[1],
            "post_content": v[2],
            "post_time": v[3],
            "title": v[4],
            "bried": v[5],
            "post_url": v[6],
            "media": "2",
            "task": "1"
        })
    print("end")
    return

def delete():
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    Posts = db.Posts
    Posts.remove()
    disaster_info_table.objects.all().delete()
    disaster_info_cache_table.objects.all().delete()

def delete2():
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    Posts = db.Posts
    now_time = datetime.datetime(2020, 2, 1, 0, 0)
    Posts.remove({"post_time": {"$gte": now_time}})

def exportDB():
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    posts = db.posts
    Posts = db.Posts
    logger.info("统计数据")
    dd = Posts.aggregate([{"$group": {"_id": "$cluster", "count": {"$sum": 1}}},
                     {"$sort": {"count": -1}}], allowDiskUse=True)

    res = []
    idx = 1
    removeChong = {}
    for d in dd:
        logger.info("循环")
        logger.info(d.get("count"))
        cluster = str(d.get("_id"))
        logger.info(cluster)
        pp = Posts.find({
            "cluster": int(cluster)
        })
        # if len(pp) < 100:
        #     continue
        texts = []
        count = 0
        for p in pp:
            logger.info("p")
            post_id = str(p.get("post_id"))
            logger.info(post_id)
            vv = posts.find({
                "post_id": post_id,
                "media": "1",
                "task": "1"
            })
            for v in vv:
                text = v.get("post_content", "")
                if removeChong.get(text, None) != None:
                    continue
                removeChong[text] = 1
                logger.info(text)
                texts.append(text)
                count = count + 1
                break
            if count == 150:
                break
        tmp = {}
        tmp["text"] = texts
        tmp["category"] = idx
        res.append(tmp)
        idx = idx + 1
        if idx == 11:
            break
    save_json("cluster-data", res)
    return

def save_json(name, obj):
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + name + '.json', 'w', encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

# def exportDB():
#     return
    # client = pymongo.MongoClient("localhost", 27017)
    # db = client.admin
    # db.authenticate('root', 'buptweb007')
    # db = client.SocialMedia
    # Posts = db.Posts.find()
    # PostsBackup = db.PostsBackup
    # PostsBackup.remove()
    #
    # print(Posts.count())
    # idx = 1
    # for p in Posts:
    #     idx += 1
    #     if p.get('task') == '1':
    #         PostsBackup.insert(p)
    #     if idx % 1000 == 0:
    #         print(idx)

def testNews():
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    collection = db.posts  # 测试
    end_time = datetime.datetime(2021, 3, 27, 0, 0)
    pp = collection.find({
        "post_time": {"$gte": end_time},
        "task": "1",
        "media": "2"
    }).count()
    # res = []
    # for p in pp:
    #     p["post_time"] = str(p["post_time"])
    #     res.append(p)
    return int(pp)

def countMongo():
    client = pymongo.MongoClient("localhost", 27017)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    # 数据总量
    start_time = datetime.datetime(2021, 11, 13, 0, 0)

    end_time = datetime.datetime(2021, 11, 30, 0, 0)
    count = db.posts.find({
        "post_time": {"$gte": start_time, "$lt": end_time},
        "task": "1",
        "media": "1"
    }).count()
    return count
    # 微博数据量
    count = db.posts.find({
        "task": "1",
        "media": "1"
    }).count()
    return count
    # 新闻数据量
    count = db.posts.find({
        "task": "1",
        "media": "2"
    }).count()

def buildExperiment():
    logger.info("开始计算热度")
    task = "1"
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    records = disaster_info_table.objects.filter(task=task).order_by("-hot")
    res = []
    for record in records:
        tmp_list = []
        if record.hot > 200 and record.hot < 1000:
            number = record.number
            posts = db.Posts.find({
                "task": task,
                "cluster": number
            })
            for post in posts:
                post_content = post.get("post_content", "")

    return

def buildTimeLineWords(media_data):
    ltp = LTP()
    ns_new = []
    nr_new = []
    nt_new = []
    terms = []
    # 所有数据，格式为   编号：[帖子时间，内容，热度]
    all_data = {}
    all_data_id = []
    re_all_data = {}
    repost_post = {}
    for c, v in enumerate(media_data):
        if c % 1000 == 0:
            logger.info(f"*************{c}*************")
        ns_new_tmp = []
        nr_new_tmp = []
        nt_new_tmp = []
        terms_tmp = []
        post_time = v.get("post_time", None)
        post_id = v.get("post_id", None)
        post_content = v.get("post_content", "")
        post_url = v.get("post_url", "")
        forward_num = 100000
        comment_num = 100000
        like_num = 100000
        hot = 0.4 * np.log(forward_num + 1) + 0.4 * np.log(comment_num + 1) + 0.2 * np.log(like_num + 1)
        if v.get("media") == "1":
            forward_num = v.get("forward_num", 0)
            comment_num = v.get("comment_num", 0)
            like_num = v.get("like_num", 0)
            if v.get("repost_id", None) != None:
                repost_id = v.get("repost_id")
                if repost_post.get(repost_id, None) == None:
                    repost_post[repost_id] = []
                repost_post[repost_id].append(post_id)
                re_all_data[post_id] = [post_time, str(post_content), hot, forward_num, comment_num, like_num, post_url]
                continue
        elif v.get("media") == "2":
            post_content = v.get("title", "")
        data = re.sub(f"<#>|#|</#>|<u>.*?</u>|<@>.*?</@>|【.*?】|地震", '', post_content)
        # sentences = re.split('(?:[。|！|\!||？|\?|；|;])', data)
        try:
            words, hidden = ltp.seg([data])
            ner_words = ltp.ner(hidden)[0]
            for nes in ner_words:
                tag, begin, end = nes
                if tag == 'Ns':
                    for idx in range(begin, end + 1):
                        word = words[0][idx]
                        if word not in ns_new_tmp:
                            ns_new_tmp.append(word)
                if tag == 'Ni':
                    for idx in range(begin, end + 1):
                        word = words[0][idx]
                        if word not in nt_new_tmp:
                            nt_new_tmp.append(word)
                if tag == 'Nh':
                    for idx in range(begin, end + 1):
                        word = words[0][idx]
                        if word not in nr_new_tmp:
                            nr_new_tmp.append(word)
            for word in words[0]:
                if word in stop_words or word in ns_new_tmp or word in nt_new_tmp or word in nr_new_tmp:
                    continue
                terms_tmp.append(word)
        except:
            logger.info(f"{data}异常")
        ns_new.append(ns_new_tmp)
        nr_new.append(nr_new_tmp)
        nt_new.append(nt_new_tmp)
        terms.append(terms_tmp)
        all_data_id.append(post_id)
        all_data[post_id] = [post_time, str(post_content), hot, forward_num, comment_num, like_num, post_url]
    id_document = dict(zip(all_data_id, range(len(all_data))))
    # 数字转字符串
    document_id = dict(zip(range(len(all_data_id)), all_data_id))
    return terms, ns_new, nr_new, nt_new, all_data, all_data_id, id_document, document_id, re_all_data, repost_post

def buildTFIDF(documents, ns_new, nr_new, nt_new):
    try:
        ns = [' '.join(i) for i in ns_new]
        ns_tfidf_model = TfidfVectorizer()
        ns_tfidf = ns_tfidf_model.fit_transform(ns)
    except:
        ns_tfidf = None
    try:
        nt = [' '.join(i) for i in nt_new]
        nt_tfidf_model = TfidfVectorizer()
        nt_tfidf = nt_tfidf_model.fit_transform(nt)
    except:
        nt_tfidf = None
    try:
        nr = [' '.join(i) for i in nr_new]
        nr_tfidf_model = TfidfVectorizer()
        nr_tfidf = nr_tfidf_model.fit_transform(nr)
    except:
        nr_tfidf = None
    try:
        documents = [' '.join(i) for i in documents]
        documents_tfidf_model = TfidfVectorizer()
        documents_tfidf = documents_tfidf_model.fit_transform(documents)
    except:
        documents = None
    # documents_id2word = {v: k for k, v in documents_tfidf_model.vocabulary_.items()}
    else:
        documents_tfidf = None
    return ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf

def buildTFIDFTimeLine(number, ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf, all_data, all_data_id, id_document, document_id, re_all_data, repost_post):
    # 查找
    sim_a = 0.15
    w1 = 1  # ns
    w2 = 1  # nr
    w3 = 1  # nt
    a = 0.6  # documents
    b = 1 - a  # entity
    time_a = 12  # 小时
    neighbourhoods = {}
    post2cluster = {}
    # 找出所有点的邻域
    cluster = 0
    logger.info(f"all_data_id的长度{len(all_data_id)}")
    for idx, now_id in enumerate(all_data_id):
        if idx % 100 == 0:
            logger.info("*****buildTFIDFTimeLine*****")
        now = id_document[now_id]
        past_list = all_data_id[:idx]
        isNewCluster = True
        max_sim = 0
        max_sim_post =""
        start = time.time()
        logger.info(f"idx:{idx}")
        for now_idx, past_id in enumerate(reversed(past_list)):
            past = id_document[past_id]
            if all_data[now_id][0] - datetime.timedelta(hours=time_a) > all_data[past_id][0]:
                logger.info(f"遍历了{now_idx}条")
                break
            sim_document = 0
            if documents_tfidf != None:
                sim_document = cosine_similarity(documents_tfidf[now], documents_tfidf[past])
            sim_entity = 0
            c = 0
            if ns_tfidf != None:
                sim_entity = sim_entity + w1 * cosine_similarity(ns_tfidf[now], ns_tfidf[past])
                c += 1
            if nr_tfidf != None:
                sim_entity = sim_entity + w2 * cosine_similarity(nr_tfidf[now], nr_tfidf[past])
                c += 1
            if nt_tfidf != None:
                sim_entity = sim_entity + w3 * cosine_similarity(nt_tfidf[now], nt_tfidf[past])
                c += 1
            sim = a * sim_document + b * sim_entity / c
            if sim > sim_a and sim > max_sim:
                isNewCluster = False
                max_sim = sim
                max_sim_post = past_id
        end = time.time()
        logger.info(f"耗时:{end - start}")
        if isNewCluster:
            cluster += 1
            if neighbourhoods.get((cluster, None)) == None:
                neighbourhoods[cluster] = []
            neighbourhoods.get(cluster).append(now_id)
            post2cluster[now_id] = cluster
        else:
            post2cluster[now_id] = post2cluster[max_sim_post]
    re_neighbourhoods = {}
    for k, v in neighbourhoods.items():
        for now_id in v:
            if repost_post.get(now_id, None) != None:
                for re_id in repost_post.get(now_id):
                    if re_neighbourhoods.get(k, None) == None:
                        re_neighbourhoods[k] = []
                    re_neighbourhoods[k].append(re_id)
    data_len = 0
    for k, v in neighbourhoods.items():
        data_len += len(neighbourhoods[k]) + len(re_neighbourhoods.get(k, []))
    if data_len < 40:
        event_num = 5
    else:
        if data_len < 200:
            event_num = 10
        else:
            event_num = 25
    events = []
    for k, v in sorted(neighbourhoods.items()):
        if len(neighbourhoods[k]) + len(re_neighbourhoods.get(k, [])) > data_len / event_num or len(neighbourhoods[k]) + len(re_neighbourhoods.get(k, [])) > 50:
            max_hot = 0
            post_id = ""
            isOri = True
            for id in neighbourhoods[k]:
                if max_hot < all_data[id][2]:
                    isOri = True
                    max_hot = all_data[id][2]
                    post_id = id

            for id in re_neighbourhoods.get(k, []):
                if max_hot < re_all_data[id][2]:
                    isOri = False
                    max_hot = re_all_data[id][2]
                    post_id = id
            if isOri:
                post_content = all_data[post_id][1]
                post_time = all_data[post_id][0]
                post_url = all_data[post_id][-1]
            else:
                post_content = re_all_data[post_id][1]
                post_time = re_all_data[post_id][0]
                post_url = re_all_data[post_id][-1]
            post_content = re.sub("(<@>.*?</@>)|#|<#>|</#>|(<u>.*?</u>)", "", post_content)
            event_time_line(content = post_content, time = post_time, url = post_url, number = number).save()
    return events


def buildTimeLine():
    logger.info("开始构建事件线")
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    number = 257
    media_data = db.Posts.find({
        "task": "1",
        "cluster": number,
    }).sort([("post_time", 1)])
    documents, ns_new, nr_new, nt_new, all_data, all_data_id, id_document, document_id, re_all_data, repost_post = buildTimeLineWords(
        media_data)
    ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf = buildTFIDF(documents, ns_new, nr_new, nt_new)
    buildTFIDFTimeLine(number, ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf, all_data, all_data_id, id_document,
                       document_id, re_all_data, repost_post)
    logger.info("257 is built")
    for k, record in enumerate(disaster_info_table.objects.all()):
        if k % 10 == 0:
            logger.info(f"##################{k}####################")
        number = record.number
        if number == 257:
            continue
        media_data = db.Posts.find({
            "task": "1",
            "cluster": number,
        }).sort([("post_time",1)])
        documents, ns_new, nr_new, nt_new, all_data, all_data_id, id_document, document_id, re_all_data, repost_post = buildTimeLineWords(media_data)
        ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf = buildTFIDF(documents, ns_new, nr_new, nt_new)
        buildTFIDFTimeLine(number, ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf, all_data, all_data_id, id_document, document_id, re_all_data, repost_post)
    return

def testMongo():
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    number = 257
    media_data = db.Posts.find({
        "task": "1",
        "cluster": number,
    }).count()
    return media_data

def timeline():
    task = "1"
    event_time_line.objects.all().delete()
    client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
    db = client.admin
    db.authenticate('root', 'buptweb007')
    db = client.SocialMedia
    records = disaster_info_table.objects.filter(task=task)
    print(len(records))
    sub_859 = []
    for record in records:
        number = int(record.number)
        # print(f"number:{number}")
        logger.info(f"number:{number}")
        # if number == 859:
        #     continue
        posts = db.Posts.find({
            "cluster": number,
            "media": "1"
        }).sort([("post_time", 1)])
        count = db.Posts.find({
            "cluster": number,
            "media": "1"
        }).count()
        print(f"事件相关数据量:{count}")
        sub_events = []
        hot_sum = 0

        post_num = 0
        for post in posts:
            if post.get("repost_id", None) != None and str(post.get("repost_id", None)) != "-100":
                continue
            forward_num = 2 * int(post.get("forward_num", 0))
            comment_num = 2 * int(post.get("comment_num", 0))
            like_num = int(post.get("like_num", 0))
            hot_sum = hot_sum + forward_num + comment_num + like_num
            post_num = post_num + 1

        posts = db.Posts.find({
            "cluster": number,
        }).sort([("post_time", 1)])
        flag = True
        for post in posts:
            if post.get("repost_id", None) != None and str(post.get("repost_id", None)) != "-100":
                continue
            forward_num = 2 * int(post.get("forward_num", 0))
            comment_num = 2 * int(post.get("comment_num", 0))
            like_num = int(post.get("like_num", 0))

            hot = forward_num + comment_num + like_num
            post_content = post.get("post_content", "")
            if len(post_content) > 990:
                continue
            post_time = post.get("post_time", "")
            post_url = post.get("post_url", "")
            post_content = re.sub("(<@>.*?</@>)|#|<#>|</#>|(<u>.*?</u>)", "", post_content)
            if post_num <= 50 and ("级地震" in post_content or "级左右地震" in post_content):
                if flag:
                    flag = False
                else:
                    continue
                try:
                    logger.info(f"添加子事件：{post_content}")
                    event_time_line(content=post_content, time=post_time, url=post_url, number=number).save()
                    sub_events.append(post_content)
                    if number == 859:
                        sub_859.append(post_content)
                except:
                    pass
            if post_num <= 50 and hot > hot_sum / 5 and like_num > 1 and judge(sub_events, post_content):
                if "级地震" in post_content or "级左右地震" in post_content:
                    if flag:
                        flag = False
                    else:
                        continue
                try:
                    logger.info(f"添加子事件：{post_content}")
                    event_time_line(content=post_content, time=post_time, url=post_url, number=number).save()
                    sub_events.append(post_content)
                    if number == 859:
                        sub_859.append(post_content)
                except:
                    pass
            if post_num > 50 and hot > hot_sum / 200 and like_num > 200 and judge(sub_events, post_content):
                if "级地震" in post_content or "级左右地震" in post_content:
                    if flag:
                        flag = False
                    else:
                        continue
                try:
                    logger.info(f"添加子事件：{post_content}")
                    event_time_line(content=post_content, time=post_time, url=post_url, number=number).save()
                    sub_events.append(post_content)
                    if number == 859:
                        sub_859.append(post_content)
                except:
                    pass

    # 859处理
    number = 859
    q = ["KgA2QzE3I", "KgAo3By77", "KgAMFsryJ", "KgAMxfrhs", "KgDXfmBJb", "KgF7DgscR", "KgF8s8d5H", "KgOTtF6rZ","KgX7rAI9F"]
    tmp = []
    for i in q:
        records = db.Posts.find({
            "task": task,
            "media": "1",
            "cluster": number,
            "post_id": i,
        })
        for r in records:
            tmp.append(r)
        for record in tmp:
            post_content = record.get("post_content", "")
            post_time = record.get("post_time", "")
            post_url = record.get("post_url", "")
            try:
                if judge(sub_859, post_content):
                    event_time_line(content=post_content, time=post_time, url=post_url, number=859).save()
            except:
                pass

def judge(sub_events, content):
    for sub_event in sub_events:
        # 并集
        union = 0
        # 交集
        intersect = 0
        for s in sub_event:
            if s in content:
                intersect = intersect + 1
        union = len(sub_event) + len(content) - intersect
        if intersect / union > 0.5:
            return False
    return True