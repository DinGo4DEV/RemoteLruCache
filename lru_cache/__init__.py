import asyncio
from asyncio.log import logger
import collections
import functools
import logging
import threading
import pickle
from typing import Any, overload
import warnings
import ujson
from gql.client import Client
from collections import OrderedDict
from asyncio.futures import Future
from lru_cache.redis import Redis
from typing import AsyncGenerator
class LRUCache:
    
    redis = Redis()
    
    def __init__(self,maxsize:int=10,ttl:int=-1,marshal=pickle):
        """Least Recently Used (LRU) cache Use for dataloader
        The cache will connect remote redis as remotecache
        
        Args:
            maxsize (int, optional): Capacity of the data in cache . Defaults to 10.
            ttl (int, optional): ttl for redis cache. Defaults to -1.
            marshal (module, optional): marshal tool for Object Serialization maybe use `ujson` , `pickle` or `json` . Defaults to pickle.

        Raises:
            TypeError: if the provied marshal doesn't has `dumps` and `loads` attributes, with raise the error
        """
        
        self.maxsize = maxsize
        self.ttl = ttl
        if getattr(marshal,"dumps") is None or getattr(marshal,"loads") is None:
            raise TypeError("Please Use a dumpable and loadable object (e.g. pickle, json, ujson)")
        self.marshal=marshal
        self.cache = OrderedDict()
        self.time_hanlders = {}
        pass
    
    # def __call__(self):
    #     try:
    #         with warnings.catch_warnings():
    #             warnings.filterwarnings(
    #                 "ignore", message="There is no current event loop"
    #             )
    #             loop = asyncio.get_event_loop()
    #     except RuntimeError:
    #         loop = asyncio.new_event_loop()
    #         asyncio.set_event_loop(loop)
    #     assert not loop.is_running(), (
    #             "Cannot run client.execute(query) if an asyncio loop is running."
    #             " Use 'await client.execute_async(query)' instead."
    #         )
    
    def get(self,id):
        if id in self.cache:
            self.cache.move_to_end(id)
            time_handler = self.time_hanlders.get(id,None)
            if time_handler:
                if not time_handler.cancelled():
                    time_handler.cancel()
                loop = asyncio.get_event_loop()  
                handler = loop.call_later(self.ttl,functools.partial(self.clear,key=id))
                self.time_hanlders.update({id:handler})
            return self.cache[id]
        try:
            data = self.redis.get(id)            
            if data is None:
                return None
            else:
                future = Future()
                future.set_result(self.marshal.loads(data))
                self.cache[id] = future
                return self.cache[id]
        except Exception as e:
            print(e)
            return None
    
    def __getitem__(self,key):
        return self.get(key)
        
    
    def __setitem__(self, name, value):
        import asyncio
        import typing
        if not isinstance(value,(typing.Awaitable,typing.Coroutine,Future)):
            future = Future(loop=asyncio.get_event_loop())
            future.set_result(value)
            value = future
        self.cache[name] = value
        self.cache.move_to_end(name)
        future:Future = value        
        if len(self.cache) > self.maxsize:
            first_key = self.cache.popitem(last = False)          
            time_handler = self.time_hanlders.get(first_key,None)
            if time_handler:
                if not time_handler.cancelled():
                    time_handler.cancel()
        loop = asyncio.get_event_loop()        
        ## If redis connection is On, Add callback funtion for Future when done
        if self.redis.active:
            future.add_done_callback(functools.partial(self._set_remote_cache,name))
        if self.ttl and self.ttl > 0:
            handler = loop.call_later(self.ttl,functools.partial(self.clear,key=name))
            self.time_hanlders.update({name:handler})
            
    def update(self, *args, **kwargs):
        self.cache.update(**kwargs)
        return self
        

    def pop(self,key,default_value=None):
        if key in self.cache:
            try:
                return self.cache.pop(key)
            except:
                return default_value
        return default_value
    
    @overload               
    def clear(self,key:Any) -> 'LRUCache': ...
    
    @overload
    def clear(self) -> 'LRUCache': ... 
    
    def clear(self,key=None)->'LRUCache':
        if key:
            self.cache.pop(key,None)
            print(f"Cleaned {key} with {self.ttl}s")            
            self.time_hanlders.pop(key,None)
            print(self.time_hanlders)
        else:
            self.cache.clear()
        return self

    def _set_remote_cache(self,name,future:Future):
        result = future.result()
        self.redis.set(name,self.marshal.dumps(result),ex=self.ttl if self.ttl and self.ttl>0 else None)

    def __len__(self):
        return len(self.cache)
    
    def __repr__(self):
        return repr(self.cache)
    