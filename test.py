# -*- coding: utf-8 -*-

# 先执行 python task.py --mq=task.mq --green=1

from task import Task
import gevent
    
t = Task(mq="task.mq", use_greenlets=True)

print t.block_echo(60, "abc")
gevent.wait()