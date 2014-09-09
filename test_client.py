# -*- coding: utf-8 -*-

# 先执行 python task.py --mq=task.mq --green=1

from task import Task
import time
    
tk = Task(mq="task.mq", use_greenlets=True)

t1 = time.time()
print tk.block_add(60, 2, 4)
print time.time() - t1
