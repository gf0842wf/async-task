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
    
    def __init__(self, host="localhost", port=6379, db=0, mq="task.mq", green=True, task_mod=None):
        self.mq = mq
        self.task_mod = task_mod or self
        self.green = green
        
        self.dumps = pickle.dumps
        self.loads = pickle.loads
        
        if green:
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
        msg = (name, block, args, kw)
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
    
    def _push_resp(self, tap, value, req_block):
        self.conn.lpush(tap, self.dumps(value))
        if not req_block:
            # 如果是nonblock调用,则会产出废list,用这个过期清除
            self.conn.expire(tap, 60)
        
    def process(self, block=True, timeout=0):
        assert block == True
        
        while True:
            data = self.resp_msg(block, timeout)
            if not data: continue
            tap, msg = data
            name, req_block, args, kw = msg
            value = getattr(self.task_mod, name)(*args, **kw)
            if self.green:
                gevent.spawn(self._push_resp, tap, value, req_block)
            else:
                self._push_resp(tap, value, req_block)
                
    def _block_echo(self, xx):
        return xx
    
if __name__ == "__main__":
    import sys
    ARGS = filter(lambda arg: isinstance(arg, list) and len(arg)==2, 
                  [arg.lstrip("-").split("=") for arg in sys.argv[1:]])
    ARGS = dict(ARGS)
    
    host = ARGS.get("host", "localhost")
    port = int(ARGS.get("port", 6379))
    db = ARGS.get("db", 0)
    mq = ARGS.get("mq", "task.mq")
    green = int(ARGS.get("green", 1))
    smod = ARGS.get("task_mod")
    lmod = smod.rsplit(".")
    task_mod = __import__(smod, globals(), locals(), lmod[1:])
    
    task = Task(host=host, port=port, db=db, mq=mq, green=green, task_mod=task_mod)
    if green:
        gevent.spawn(lambda: task.process(block=True, timeout=0))
        gevent.wait()
    else:
        task.process(block=True, timeout=0)
        
    # pypy task.py --mq=task.mq --green=1 --task_mod=sample.mytask
    # 最好启动 ncpu 个实例, 注意此命令的执行目录, 注意task_mod的格式 sample.mytask表示 from sample import mytask
