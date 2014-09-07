# -*- coding: utf-8 -*-

from gevent import socket
import redis
import redis.connection
import uuid
import cPickle as pickle


class TaskError(Exception):
    pass


class Task(object):
    
    def __init__(self, host='localhost', port=6379, db=0, use_greenlets=True):
        if use_greenlets:
            redis.connection.socket = socket
        self.conn = redis.StrictRedis(host=host, port=port, db=db)

    def req_msg(self, msg, block=False):
        """@param tap: 消息队列标识(域.具体名)
           : 把请求的消息放在list task.mq 里
           : 把响应的消息放在list task.tap 里
        """
        tap = "task."+uuid.uuid1().hex
        data = pickle.dumps((tap, msg))
        
        self.conn.lpush("task.mq", data)
        if block:
            return self.conn.brpop(tap)
        
    def process_msg(self, block=True, timeout=0):
        """@param timeout: 当block=True时才有用
           : 处理消息可以是外部别的语言写的程序,可以开多个消费者来处理消息
        """
        if block:
            _data = self.conn.brpop("task.mq", timeout)
            if not _data: raise TaskError("timeout")
            return
        else:
            _data = self.conn.rpop("task.mq")
            return
        
        _, data = _data
        
        tap, msg = pickle.loads(data)
        
        self.conn.lpush(tap, "flag.ack.done")
        
        return (tap, msg)
    
    
if __name__ == "__main__":
    import gevent
    
    t = Task(use_greenlets=True)

    def f1():
        print t.req_msg("fuck", block=True)
        
    def f2():
        print t.process_msg(block=True, timeout=120)
        
    gevent.spawn(f2)
    gevent.spawn(f1)

    gevent.wait()
