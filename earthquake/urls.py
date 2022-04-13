#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/10/6 15:39
# @Author  : hui
# @Email   : huihuil@bupt.edu.cn
# @File    : urls.py
from django.contrib import admin
from django.urls import path
from . import views
app_name = 'earthquake'
urlpatterns = [
    path('test',views.test),
    path('test2',views.test2),
    path('webAddHot', views.webAddHot),
    path('bentchmark', views.bentchmark),
    path('buildData', views.buildData),
]
