### async task ###

- 执行命令
		
		因为主要代码是gevent写的,所以需要rpc执行的是cpu密集计算,所以使用pypy

		pypy task.py --mq=task.mq --green=1 --task_mod=sample.mytask