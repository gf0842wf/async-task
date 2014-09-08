# -*- coding: utf-8 -*-

# 先执行 python task.py --mq=task.mq --green=1

from task import Task
import gevent
    
t = Task(mq="task.mq", use_greenlets=True)

print t.block_add(60, "abc", "def")

gevent.wait()