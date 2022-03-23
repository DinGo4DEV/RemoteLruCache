
from typing import Optional, Union
import redis
from redis import Redis, ConnectionPool
from redis.exceptions import ConnectionError

import asyncio
from functools import wraps
import logging

ConfigFile = dict

logger= logging.getLogger(__name__)

class Redis(redis.Redis):
    class Redis(Redis):
        active = True
    
    @classmethod
    async def set_after(cls):
        while not cls.active:
            await asyncio.sleep(1)
            try:
                if cls.ping():
                    cls.active = True
            except:
                return None
    
    def __init__(self,host:Optional[str]="127.0.0.1",port:Optional[int]=6379,db:Optional[int]=0,decode_responses=True,config:Union[dict,ConfigFile,None]=None):
        if isinstance(config,(dict,ConfigFile)):
            redis_config = config['redis']
            pool = ConnectionPool(host=redis_config['host'], port=redis_config['port'], db=redis_config['database'], decode_responses=True)
        else:
            pool = ConnectionPool(host=host,port=port,db=db,decode_responses=decode_responses)
        super().__init__(connection_pool=pool)
        try:
            if self.ping() :
                Redis.active = True
        except ConnectionError as e:
            logger.warning("Redis: %r",e)
            Redis.active = False
    
    # @measure
    def get(self,name):
        return super().get(name)