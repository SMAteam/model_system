import datetime
import json

import pymongo
import redis
from django.shortcuts import render
from .models import disaster_info
from django.http import HttpResponse
from django.db import connection as conn

from .newTasks import eventDetect, getTestLocation
from .tasks import earthquakeDetect, xinwen, weibo, earthquakeDetectQueue, redisCache, exportDB, delete, testNews, \
    countMongo, addHot, buildTimeLine, testMongo, timeline
from .models import disaster_info

import logging
# 获得logger实例
# from .tests import testTime


def test(request):
    # earthquakeDetect()
    # redisCache()
    return HttpResponse()
# Create your views here.
def test2(request):

    # exportDB()


    # delete()
    # eventDetect()
    # redisCache()


    # exportDB()
    #
    # getTestLocation()
    #　return HttpResponse(json.dumps(countMongo(), ensure_ascii=False))
    # return HttpResponse(json.dumps(testNews(), ensure_ascii=False))
    # n = testMongo()
    timeline()
    return HttpResponse("success")
    # return HttpResponse(countMongo())

def webAddHot(request):
    # addHot()
    return HttpResponse("success")

def bentchmark(request):
    # testTime()
    return HttpResponse("success")

def buildData(request):
    buildTimeLine()
    return HttpResponse("success")