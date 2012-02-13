# wrappers for pyuv

from threading import Thread,Event
import logging
from functools import partial

import pyuv
from tornado.ioloop import IOLoop


class _loop(object):
    """A loop object that runs in a separate thread."""
    __loop = None
    __th = None
    __thstop = None
    
    @classmethod
    def init(cls):
        cls.__loop = pyuv.Loop.default_loop()
        cls.__th = Thread(target=cls._thread)
        cls.__thstop = Event()
        cls.__event = Event()
        
        cls.start()
    
    @classmethod
    def start(cls):
        # start pyuv thread
        cls.__event.clear()
        cls.__thstop.set()
        cls.__th.start()
    
    @classmethod
    def run(cls):
        if not cls.__th.is_alive():
            raise Exception('pyuv loop not running')
        cls.__event.set()
    
    @classmethod
    def getloop(cls):
        return cls.__loop

    @classmethod
    def stop(cls):
        cls.__thstop.clear()
        cls.__event.set()
        cls.__thstop.wait(1)
    
    @classmethod
    def _thread(cls):
        while cls.__thstop.is_set():
            # wait for a loop event, then run the loop
            cls.__event.wait()
            cls.__event.clear()
            cls.__loop.run()
        
        # if stopped, set flag to wake up caller
        cls.__thstop.set()

class Metafs(type):
    """Wrapper around the pyuv.fs module"""
    
    _fsfuncs = [x for x in dir(pyuv.fs) if callable(getattr(pyuv.fs,x)) and not isinstance(getattr(pyuv.fs,x),type)]
    _fsconst = [x for x in dir(pyuv.fs) if x[:2] != '__' and not callable(getattr(pyuv.fs,x))]
    
    @classmethod
    def __getattr__(cls,name):
        if name in cls._fsfuncs:
            return partial(cls.f,name)
        elif name in cls._fsconst:
            return getattr(pyuv.fs,name)
        #elif name == 'FSEvent':
        #    return cls.FSEvent
        else:
            raise Exception('%s is not a fs object'%name)
    
    @classmethod
    def f(cls,name,*args,**kwargs):
        ret = None
        if 'callback' in kwargs and kwargs['callback'] is not None:
            callback = kwargs.pop('callback')
            def cb1(loop,*args,**kwargs):
                IOLoop.instance().add_callback(partial(callback,*args,**kwargs))
            ret = getattr(pyuv.fs,name)(_loop.getloop(),*args,callback=cb1,**kwargs)
        else:
            logging.info('no callback')
            ret = getattr(pyuv.fs,name)(_loop.getloop(),*args,**kwargs)
        _loop.run()
        return ret
        
    #@classmethod
    #def FSEvent(cls):
    #    class _FSEvent():
    #        def __init__(self):
    #            self.__f = pyuv.fs.FSEvent(_loop.getloop())
    #        def __getattr__(self,name):
    #            if name in ('start','close'):
    #                return partial(self.__run,name)
    #            elif name == 'loop':
    #                return _loop.getloop()
    #            elif name == 'data':
    #                return self.__f.data
    #        def __run(self,name,callback=None,*args,**kwargs):                
    #            if callback is not None:
    #                def cb(loop,*args,**kwargs):
    #                    IOLoop.instance().add_callback(partial(callback,*args,**kwargs))
    #                getattr(self.__f,name)(self,*args,callback=cb,**kwargs)
    #            else:
    #                getattr(self.__f,name)(self,*args,**kwargs)
    #    return _FSEvent()

class fs(object):    
    __metaclass__ = Metafs
    def __getattr__(self,name):
        return getattr(fs,name)


# start the pyuv loop
_loop.init()
