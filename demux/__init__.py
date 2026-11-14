"""后台拆样（demultiplexing）模块。

* :mod:`demux.matcher`     两端 4 碱基条码匹配（每端最多 1 处替换）
* :mod:`demux.persistence` 拆样任务的 SQLite 持久层
* :mod:`demux.service`     后台工作线程、断点续跑与崩溃安全发布
* :mod:`demux.app`         FastAPI 路由（挂载到 collector 进程）
"""
