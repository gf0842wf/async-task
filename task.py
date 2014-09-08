# -*- coding: utf-8 -*-

from gevent import socket as green_socket
import redis
import redis.connection
import uuid
import cPickle as pickle
import gevent


class TaskError(Exception):
    pass


class Task(object):
    
    def __init__(self, host="localhost", port=6379, db=0, mq="task.mq", use_greenlets=True, task_mod=None):
        self.mq = mq
        self.task_mod = task_mod or self
        self.use_greenlets = use_greenlets
        
        self.dumps = pickle.dumps
        self.loads = pickle.loads
        
        if use_greenlets:
            redis.connection.socket = green_socket
        self.conn = redis.StrictRedis(host=host, port=port, db=db)

    def req_msg(self, msg, block=False, timeout=0):
        """请求消息
           : 把请求的消息放在list self.mq 里
           : 把响应的消息放在list task.tap 里
        """
        tap = "task."+uuid.uuid1().hex
        data = self.dumps((tap, msg))
        
        self.conn.lpush(self.mq, data)
        if block:
            data = self.conn.brpop(tap, timeout)
            if not data:
                raise TaskError("timeout")
                return
            tap, _value = data
            return self.loads(_value)
    
    def call(self, name, block, timeout, args=(), kw={}):
        msg = (name, args, kw)
        return self.req_msg(msg, block, timeout)
    
    def __getattr__(self, name):
        """ .block_echo(120, "abc") # 第一个参数是timeout,只有是block时才有效,0是不超时
        """
        if name.startswith("block_"):
            return lambda timeout, *args, **kw: self.call("_"+name, True, timeout, args, kw)
        elif name.startswith("nonblock_"):
            return lambda timeout, *args, **kw: self.call("_"+name, False, timeout, args, kw)
        else:
            return getattr(self, name)
        
    def resp_msg(self, block, timeout):
        """回复消息
           @param timeout: 当block=True时才有用
           : 处理消息可以是外部别的语言写的程序,可以开多个消费者来处理消息
        """
        if block:
            _data = self.conn.brpop(self.mq, timeout)
            if not _data:
                raise TaskError("timeout")
                return
        else:
            _data = self.conn.rpop(self.mq)
            return
        
        _, data = _data
        
        tap, msg = self.loads(data)
        
        return (tap, msg)
    
    def _green_push(self, tap, value):
        self.conn.lpush(tap, self.dumps(value))
        self.conn.expire(tap, 3600) # 如果是nonblock调用,则会产出废list,用这个过期清除
        
    def process(self, block=True, timeout=0):
        assert block == True
        
        while True:
            data = self.resp_msg(block, timeout)
            if not data: continue
            tap, msg = data
            name, args, kw = msg
            value = getattr(self.task_mod, name)(*args, **kw)
            if self.use_greenlets:
                gevent.spawn(self._green_push, tap, value)
            else:
                self._green_push(tap, value)
                
    def _block_echo(self, xx):
        return xx
    
if __name__ == "__main__":
    import sys
    ARGS = filter(lambda arg: isinstance(arg, list) and len(arg)==2, 
                  [arg.lstrip("-").split("=") for arg in sys.argv[1:]])
    ARGS = dict(ARGS)
    
    mq = ARGS.get("mq", "task.mq")
    green = int(ARGS.get("green", 1))
    smod = ARGS.get("task_mod")
    lmod = smod.rsplit(".")
    task_mod = __import__(smod, globals(), locals(), lmod[1:])
    
    if green:
        t = Task(mq=mq, use_greenlets=green, task_mod=task_mod)
        gevent.spawn(lambda: t.process(block=True, timeout=0))
        gevent.wait()
    else:
        t = Task(mq=mq, use_greenlets=green, task_mod=task_mod)
        t.process(block=True, timeout=0)
    # pypy task.py --mq=task.mq --green=1 --task_mod=sample.mytask
    # 最好启动 ncpu 个实例, 注意此命令的执行目录, 注意task_mod的格式 sample.mytask表示 from sample import mytask
