from lru_cache import LRUCache
import time

cache = LRUCache(10,2)

cache['a'] = 'a'
print(cache)
time.sleep(2)
cache['b'] = 'b'
print(cache)
time.sleep(1)
print(cache)