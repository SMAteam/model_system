from django.db import models

class disaster_info(models.Model):
    province = models.CharField(max_length=30)
    city = models.CharField(max_length=30)
    area = models.CharField(max_length=30)
    time = models.DateTimeField(null=True, blank=True)
    info = models.CharField(max_length=255)
    number = models.IntegerField()
    grade = models.CharField(max_length=30)
    task = models.CharField(max_length=30)
    authority = models.CharField(max_length=10)
    post_id = models.CharField(max_length=30)
    hot = models.IntegerField()
    # 接下来设置联合主键
    class Meta:
        db_table = "disaster_info"

class disaster_info_cache(models.Model):
    province = models.CharField(max_length=30)
    city = models.CharField(max_length=30)
    area = models.CharField(max_length=30)
    post_time = models.DateTimeField(null=True, blank=True)
    info = models.CharField(max_length=255)
    post_id = models.CharField(max_length=30)
    task = models.CharField(max_length=30)
    media  = models.CharField(max_length=30)
    # 接下来设置联合主键
    class Meta:
        db_table = "disaster_info_cache"

class parameter(models.Model):
    task_id = models.CharField(max_length=10)
    last_time_weibo = models.DateTimeField(null=True, blank=True)
    last_time_xinwen = models.DateTimeField(null=True, blank=True)
    class Meta:
        db_table = "parameter"
class post_extra(models.Model):
    post_id = models.CharField(max_length=30)
    cluster = models.IntegerField()
    task = models.CharField(max_length=30)
    media  = models.CharField(max_length=30)
    class Meta:
        db_table = "post_extra"

class event_time_line(models.Model):
    content = models.CharField(max_length=1000)
    url = models.CharField(max_length=200)
    time = models.DateTimeField(null=True, blank=True)
    number  = models.IntegerField()
    class Meta:
        db_table = "event_time_line"