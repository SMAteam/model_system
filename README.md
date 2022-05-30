# 模型处理层
## 重要文件
- settings.py:系统配置文件
- task.py:系统模型处理文件
    - earthquakeDetect()//设定时间区间，读取一定时间区间内的数据进行事件检测分析
    - earthquakeDetectQueue()//实时读取RabbitMQ数据进行事件检测分析
- views.py:web函数文件
## 部署步骤
- 系统启动命令：nohup python manage.py runserver 0.0.0.0:8001 > model.log
    - 1.离线检测：通过8001端口访问views.py内的web函数调用earthquakeDetect()启动事件检测分析
    - 2.实时检测：
        - nohup python manage.py celery beat > beat.log &
        - nohup python manage.py celery worker > worker.log &
        - 配置model_system\settings.py内CELERYBEAT_SCHEDULE时间控制事件实时检测开始时间

