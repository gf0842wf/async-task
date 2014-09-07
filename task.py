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

    def push_msg(self, msg, block=False):
        """@param tap: 消息队列标识(域.具体名)
        """
        tap = "task."+uuid.uuid1().hex
        data = pickle.dumps((tap, msg))
        
        self.conn.lpush("mq.task", data)
        if block:
            return self.conn.brpop(tap)
        
    def pull_msg(self, block=True, timeout=0):
        """@param timeout: 当block=True时才有用
        """
        if block:
            _data = self.conn.brpop("mq.task", timeout)
        else:
            _data = self.conn.rpop("mq.task")
        
        if not _data and block:
            raise TaskError("timeout")
        
        _, data = _data
        
        tap, msg = pickle.loads(data)
        
        self.conn.lpush(tap, "flag.ack")
        
        return (tap, msg)
    
    
if __name__ == "__main__":
    import gevent
    
    t = Task(use_greenlets=True)

    def f1():
        print t.push_msg("fuck", block=True)
        
    def f2():
        print t.pull_msg(block=True, timeout=120)
        
    gevent.spawn(f2)
    gevent.spawn(f1)

    gevent.wait()
