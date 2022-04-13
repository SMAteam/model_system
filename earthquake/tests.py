import json
import os
import pickle
import re
import logging
# 获得logger实例
from datetime import datetime

import pymongo
from pandas import np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# from earthquake.tasks import stop_words

logger = logging.getLogger('log')
import time
import psutil
from queue import Queue
# import pandas as pd
# # # Create your tests here.
from ltp import LTP
# from snownlp import SnowNLP
# import jieba

# import jieba.analyse
# from pyhanlp import *
# import thulac
import hanlp

del_words = "发生.{0,4}级|.{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|\(|\)|）|（|,|，|;|；|。|\?|？|！|!|:|：↓|【|】|\[|\]|'|#|<@>.*?</@>|<#>|</#>|http.*?</u>|<u>"
posts_a = 100
begin_dis = 10
end_dis = 10
key_word = '地震'

def save_obj(obj, name):
    with open(os.path.dirname(os.path.abspath(__file__))+ '/' + 'resources/' + name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
def load_obj(name):
    with open(os.path.dirname(os.path.abspath(__file__))+ '/' + 'resources/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

def load_json(name):
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + name + '.json', 'rb') as f:
        return json.load(f)

def save_json(name, obj):
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + name + '.json', 'w', encoding="utf-8") as f:
        json.dump(obj, f)

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
    def getLocationTest(self, s):
        ner_dict = self.run(s)
        return len(ner_dict) != 0
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

def testLocation():

    # print(sys.getsizeof(ner))
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + "test_location" + '.json', 'rb') as f:
        posts = json.load(f)[:10005]
    start_time = time.time()
    idx = 0
    res = []
    mem = psutil.virtual_memory()
    ysy = float(mem.used)
    print(ysy / 1024 / 1024)
    print(8530382848 / 1024 / 1024)

    ner = ACTrie()
    for post in posts:
        idx +=1
        ner.getLocationTest(post)
        mem = psutil.virtual_memory()
        ysy = float(mem.used / 1024 / 1024)
        print(ysy)
        if idx % 500 == 0:
            res.append(idx)
            res.append(time.time() - start_time)
            print(idx, ":", time.time() - start_time)
    # print("深度学习")
    # ltp = LTP()
    # start_time = time.time()
    # idx = 0
    # for post in posts:
    #     idx += 1
    #     seg, hidden = ltp.seg([post])
    #     ner_words = ltp.ner(hidden)[0]
    #     mem = psutil.virtual_memory()
    #     ysy = float(mem.used)
    #     print(ysy)
    #     if idx % 500 == 0:
    #         res.append(idx)
    #         res.append(time.time() - start_time)
    #         print(idx, ":", time.time() - start_time)
    # with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + "test_location_res" + '.json', 'w') as f:
    #     json.dump(res, f)
    return

# def testTime():
#     print(f"******")
#     with open(os.path.dirname(os.path.abspath(__file__)) + '/' + 'resources/' + "test_location" + '.json', 'rb') as f:
#
#         print(f"******")
#         posts = json.load(f)[:10000]
#     print(f"******")
#     start_time = time.time()
#     print(f"******")
#     idx = 0
#     ner = ACTrie()
#     for post in posts:
#         idx += 1
#         ner.getLocationTest(post)
#     end_time = time.time()
#     print(f"自编写工具包耗时{end_time - start_time}")
#     start_time = time.time()
#     ltp = LTP()
#     for post in posts:
#         idx += 1
#         seg, hidden = ltp.seg([post])
#         ner_words = ltp.ner(hidden)[0]
#         if idx % 1000 == 0:
#             print("********")
#     end_time = time.time()
#     print(f"ltp工具包耗时{end_time - start_time}")
#     start_time = time.time()
#     for post in posts:
#         idx += 1
#         s = SnowNLP(post)
#         s.tags
#         if idx % 1000 == 0:
#             print("********")
#     end_time = time.time()
#     print(f"snownlp工具包耗时{end_time - start_time}")
#     start_time = time.time()
#     for post in posts:
#         idx += 1
#         jieba.analyse.extract_tags(post, topK=10, withWeight=True, allowPOS='ns')
#         if idx % 1000 == 0:
#             print("********")
#     end_time = time.time()
#     print(f"jieba工具包耗时{end_time - start_time}")
#     start_time = time.time()
#     thu1 = thulac.thulac()  # 默认模式
#     for post in posts:
#         idx += 1
#         text = thu1.cut(post, text=True)
#         if idx % 1000 == 0:
#             print("********")
#     end_time = time.time()
#     print(f"thulac工具包耗时{end_time - start_time}")
#     CRFLexicalAnalyzer = JClass("com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer")
#     analyzer = CRFLexicalAnalyzer()
#     start_time = time.time()
#     for post in posts:
#         idx += 1
#         analyzer.analyze(post)
#         if idx % 1000 == 0:
#             print("********")
#     end_time = time.time()
#     print(f"hanlp工具包耗时{end_time - start_time}")
#     return


def testRe():
    s = "正式测定：1月1日2时48分在四川自贡市荣县（）发生3.4级地震，震源深度9千米速报@震长"
    stop_others = ".{0,4}地震台网|北京时间|北纬.{0,6}度，东经.{0,6}度|<@>.*?</@>|<#>.*?</#>"
    topics = re.findall("<#>(.*?)</#>", s)
    for i in topics:
        print(i)
        pass
    print(s)


def testInfo():
    # 提取灾害信息
    return





# def buildTimeLineWords(media_data):
#     ltp = LTP()
#     ns_new = []
#     nr_new = []
#     nt_new = []
#     terms = []
#     # 所有数据，格式为   编号：[帖子时间，内容，热度]
#     all_data = {}
#     all_data_id = []
#     re_all_data = {}
#     repost_post = {}
#     for c, v in enumerate(media_data):
#         if c % 1000 == 0:
#             logger.info(f"*************{c}*************")
#         ns_new_tmp = []
#         nr_new_tmp = []
#         nt_new_tmp = []
#         terms_tmp = []
#         post_time = v.get("post_time", None)
#         post_id = v.get("post_id", None)
#         post_content = v.get("post_content", "")
#         forward_num = 100000
#         comment_num = 100000
#         like_num = 100000
#         hot = 0.4 * np.log(forward_num + 1) + 0.4 * np.log(comment_num + 1) + 0.2 * np.log(like_num + 1)
#         if v.get("media") == "1":
#             forward_num = v.get("forward_num", 0)
#             comment_num = v.get("comment_num", 0)
#             like_num = v.get("like_num", 0)
#             if v.get("repost_id", None) != None:
#                 repost_id = v.get("repost_id", None)
#                 repost_post.get(repost_id, []).append(post_id)
#                 re_all_data[post_id] = [repost_id, str(post_content), hot, forward_num, comment_num, like_num]
#                 continue
#         elif v.get("media") == "2":
#             post_content = v.get("title", "")
#         data = re.sub(f"<#>|#|</#>|<u>.*?</u>|<@>.*?</@>|【.*?】|地震", '', post_content)
#         # sentences = re.split('(?:[。|！|\!||？|\?|；|;])', data)
#         try:
#             words, hidden = ltp.seg([data])
#             ner_words = ltp.ner(hidden)[0]
#             for nes in ner_words:
#                 tag, begin, end = nes
#                 if tag == 'Ns':
#                     for idx in range(begin, end + 1):
#                         word = words[0][idx]
#                         if word not in ns_new_tmp:
#                             ns_new_tmp.append(word)
#                 if tag == 'Ni':
#                     for idx in range(begin, end + 1):
#                         word = words[0][idx]
#                         if word not in nt_new_tmp:
#                             nt_new_tmp.append(word)
#                 if tag == 'Nh':
#                     for idx in range(begin, end + 1):
#                         word = words[0][idx]
#                         if word not in nr_new_tmp:
#                             nr_new_tmp.append(word)
#             for word in words[0]:
#                 if word in stop_words or word in ns_new_tmp or word in nt_new_tmp or word in nr_new_tmp:
#                     continue
#                 terms_tmp.append(word)
#         except:
#             logger.info(f"{data}异常")
#         ns_new.append(ns_new_tmp)
#         nr_new.append(nr_new_tmp)
#         nt_new.append(nt_new_tmp)
#         terms.append(terms_tmp)
#         all_data_id.append(post_id)
#         all_data[post_id] = [post_time, str(post_content), hot, forward_num, comment_num, like_num]
#     id_document = dict(zip(all_data_id, range(len(all_data))))
#     # 数字转字符串
#     document_id = dict(zip(range(len(all_data_id)), all_data_id))
#     return terms, ns_new, nr_new, nt_new, all_data, all_data_id, id_document, document_id, re_all_data, repost_post
#
# def buildTFIDF(documents, ns_new, nr_new, nt_new):
#     if len(ns_new) != 0:
#         ns = [' '.join(i) for i in ns_new]
#         ns_tfidf_model = TfidfVectorizer()
#         ns_tfidf = ns_tfidf_model.fit_transform(ns)
#     else:
#         ns_tfidf = None
#     if len(nt_new) != 0:
#         nt = [' '.join(i) for i in nt_new]
#         nt_tfidf_model = TfidfVectorizer()
#         nt_tfidf = nt_tfidf_model.fit_transform(nt)
#     else:
#         nt_tfidf = None
#     if len(nr_new) != 0:
#         nr = [' '.join(i) for i in nr_new]
#         nr_tfidf_model = TfidfVectorizer()
#         nr_tfidf = nr_tfidf_model.fit_transform(nr)
#     else:
#         nr_tfidf = None
#     if len(documents) != 0:
#         documents = [' '.join(i) for i in documents]
#         documents_tfidf_model = TfidfVectorizer()
#         documents_tfidf = documents_tfidf_model.fit_transform(documents)
#     # documents_id2word = {v: k for k, v in documents_tfidf_model.vocabulary_.items()}
#     else:
#         documents_tfidf = None
#     return ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf
#
def buildTFIDFTimeLine(ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf, all_data, all_data_id, id_document, document_id, db, re_all_data, repost_post):
    # 查找
    sim_a = 0.45
    w1 = 1  # ns
    w2 = 1  # nr
    w3 = 1  # nt
    a = 0.6  # documents
    b = 1 - a  # entity
    time_a = 18  # 小时
    neighbourhoods = {}
    post2cluster = {}
    # 找出所有点的邻域
    cluster = 0
    for now_id in all_data_id:
        now = id_document[now_id]
        past_list = all_data_id[:c]
        isNewCluster = True
        for past_id in reversed(past_list):
            past = id_document[past_id]
            if all_data[now_id][0] - datetime.timedelta(hours=time_a) > all_data[past_id][0]:
                break
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
            if sim > sim_a:
                isNewCluster = False
                post2cluster[now_id] = post2cluster[past_id]
                break
        if isNewCluster:
            cluster += 1
            neighbourhoods.get(cluster, []).append(now_id)
            post2cluster[now_id] = cluster
    logger.info(f"post2cluster: {post2cluster}")
    logger.info(neighbourhoods)
    re_neighbourhoods = {}
    for k, v in neighbourhoods.items():
        for now_id in v:
            if repost_post.get(now_id, None) != None:
                for re_id in repost_post.get(now_id):
                    if re_neighbourhoods.get(k, None) == None:
                        re_neighbourhoods[k] = []
                    re_neighbourhoods[k].append(re_id)
    return neighbourhoods
#
# def buildTimeLine():
#     logger.info("开始构建事件线")
#     client = pymongo.MongoClient(host="localhost", port=27017, maxPoolSize=50)
#     db = client.admin
#     db.authenticate('root', 'buptweb007')
#     db = client.SocialMedia
#     media_data = db.Posts.find({
#         "task": "1",
#         "cluster": 257,
#     }).sort([("post_time",1)])
#     documents, ns_new, nr_new, nt_new, all_data, all_data_id, id_document, document_id, re_all_data, repost_post = buildTimeLineWords(media_data)
#     ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf = buildTFIDF(documents, ns_new, nr_new, nt_new)
#     buildTFIDFTimeLine(ns_tfidf, nt_tfidf, nr_tfidf, documents_tfidf, all_data, all_data_id, id_document, document_id, re_all_data, repost_post)
#     return

if __name__ == '__main__':
    s = "地震"
    s = re.sub(f"<#>|#|</#>|<u>.*?</u>|<@>.*?</@>|【.*?】|地震", '', s)
    print(s)
    # recognizer = hanlp.load(hanlp.pretrained.ner.MSRA_NER_BERT_BASE_ZH)
    # words = tokenizer("我想要逃课")
    # for word in words:
    #     print(word)
    # res = load_json("test_location_res")
    # idx = 0
    # for v in res:
    #     idx += 1
    #     if idx % 4 == 3:
    #         print(v)
