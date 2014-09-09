# -*- coding: utf-8 -*-

# 先执行 python task.py --mq=task.mq --green=1

from task import Task
import gevent
    
t = Task(mq="task.mq", use_greenlets=True)

import time
t1 = time.time()
print t.block_add(60, 2, 4)
print time.time() - t1
gevent.wait()