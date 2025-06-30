import hashlib
import random
import threading
import time

class CuckooHashTable:

    def __init__(self, init_capacity=1024, max_iter=8, load_factor=0.5):
        self.capacity = init_capacity
        self.max_iter = max_iter
        self.load_factor = load_factor
        self.size = 0

        self.table1 = [None] * self.capacity
        self.table2 = [None] * self.capacity

        self.seed1 = random.randint(1, 2**32-1)
        self.seed2 = random.randint(1, 2**32-1)

        #for thread safety (idk)
        self.lock = threading.RWLock() if hasattr(threading, 'RWLOCK') else threading.Lock()

        #statistics, for monitoring
        self.stats = {
            'inserts': 0,
            'lookups': 0,
            'evictions': 0,
            'rehashes': 0,
            'collisions': 0
        }

    def _hash1(self, key) -> int:
        return hash((key, self.seed1)) % self.capacity
    
    def _hash2(self, key) -> int:
        return hash((key, self.seed2)) % self.capacity
    
    def _needs_resize(self) -> bool:
        return self.size >= int(self.load_factor * self.capacity)
    
    def _resize(self):
        t_table1 = self.table1
        t_table2 = self.table2
        t_cap = self.capacity

        self.capacity *= 2
        self.table1 = [None] * self.capacity
        self.table2 = [None] * self.capacity

        #shuffle seeds
        self.seed1 = random.randint(1, 2**32-1)
        self.seed2 = random.randint(1, 2**32-1)

        old_size = self.size
        self.size = 0

        #Rehash all elements

        for table in [t_table1, t_table2]:
                for item in table:
                     if item is not None:
                          key, value, _ = item
                          self._insert_internal(key, value)

        self.stats['rehashes'] += 1
        print(f"Resized old tables to {self.capacity}")

    def _insert_internal(self, key, value):
         """Inserts into table"""
         if self._needs_resize():
              self._resize()

        #attempt to insert into table1
         pos1 = self._hash1(key)
         if self.table1[pos1] is None:
              self.table1[pos1] = (key, value, time.time())
              self.size += 1
              return True
         
        # if occupied see if it's same key & update
         if self.table1[pos1][0] == key:
              self.table1[pos1] = (key, value, time.time())
              return True
         
        #try table2
         pos2 = self._hash2(key)
         if self.table2[pos2] is None:
              self.table2[pos1] = (key, value, time.time())
              self.size += 1
              return True
         
        #if both busy
         if self.table2[pos2][0] == key:
              self.table2[pos2] = (key, value, time.time())
              return True
         

        #otherwise do eviction
         return self._evict(key, value)
    
    def _evict(self, key, value):
         """do the eviction, we bounce here between table1 and table2...fun!"""
         curr_key, curr_value = key, value

         for i in range(self.max_iter):
              
              pos1 = self._hash1(key)
              if self.table1[pos1] is None:
                self.table1[pos1] = (key, value, time.time())
                self.size += 1
                return True
              
              evicted_key, evicted_value, _ = self.table1[pos1]
              self.table1[pos1] = (curr_key, curr_value, time.time())
              curr_key, curr_value = evicted_key, evicted_value
              self.stats['evictions'] += 1

              pos2 = self._hash2(key)
              if self.table2[pos2] is None:
                  self.table2[pos2] = (key, value, time.time())
                  if i == 0:
                    self.size += 1
                  return True
              
              evicted_key, evicted_value, _ = self.table2[pos2]
              self.table2[pos2] = (curr_key, curr_value, time.time())
              curr_key, curr_value = evicted_key, evicted_value
              self.stats['evictions'] += 1

         self._resize()
         return self._internal_insert(key, value)
    
    def put(self, key, value):
         with self.lock:
              self.stats['insertions'] += 1
              self._insert_internal(key, value)
         return
    
    def get(self, key):
         with self.lock:
              self.stats['lookups'] += 1

              pos1 = self._hash1(key)
              if self.table1[pos1] is not None and self.table1[pos1][0] == key:
                   return self.table1[pos1][1]

              pos2 = self._hash2(key)
              if self.table2[pos2] is not None and self.table2[pos2][0] == key:
                   return self.table2[pos2][1]
         
    
    def contains(self, key) -> bool:
         return self.get(key) is not None
    
    def delete(self, key):
         with self.lock:
              pos1 = self._hash1(key)
              if self.table1[pos1] is not None and self.table1[pos1][0] == key:
                   self.table1[pos1] = None
                   self.size -= 1
                   return True

              pos2 = self._hash2(key)
              if self.table2[pos2] is not None and self.table2[pos2][0] == key:
                   self.table2[pos2] = None
                   self.size -= 1
                   return True
              
         return False

    def get_stats(self):
         load_factor = self.size / self.capacity if self.capacity > 0 else 0

         return {
              **self.stats,
              'size': self.size,
              'capacity': self.capacity,
              'load_factor': self.load_factor
         }
    
    def clear(self):
         
         return
    
    def keys(self):
         keys = []
         with self.lock:
              for table in [self.table1, self.table2]:
                   for item in table:
                        if item is not None:
                             keys.append(item[0])

         return keys
    
    def items(self):
         items = []
         with self.lock:
              for table in [self.table1, self.table2]:
                   for item in table:
                        if item is not None:
                             items.append(item[1])

         return items
    
    def __len__(self):
         return self.size
    
    def __contains__(self, key):
         return self.contains(key)
    
    def __getitem__(self, key):
         value = self.get(key)
         if value is None:
              raise KeyError(key)
         return value
    
    def __setitem__(self, key, value):
         self.put(key, value)

    def __delitem__(self, key):
         if not self.delete(key):
              raise KeyError(key)
         

class RWLock:
        
        def __init__(self):
            self._read_ready = threading.Condition(threading.RLock())
            self._readers = 0

        def acquire_read(self):
             self._read_ready.acquire()
             try:
                  self._readers += 1
             finally:
                  self._read_ready.release()
               
        def release_read(self):
             self._read_ready.acquire()

             try:
                  self._readers -= 1
                  if self._readers == 0:
                       self._read_ready.notifyAll()
             finally:
                  self._read_ready.release()

        def acquire_write(self):
             self._read_ready.acquire()
             while self._readers > 0:
                  self._read_ready.wait()

        def release_write(self):
             self._read_ready.release()

        def __enter__(self):
             self.acquire_write()
             return self
        
        def __exit__(self):
             self.release_write()

    #will add some other bullshit here later

if not hasattr(threading, 'RWLock'):
    threading.RWLock = RWLock


class UserFeatureStore(CuckooHashTable):
     
     def __init__(self, init_capacity=512):
          super().__init__(init_capacity)
          self.feature_schema = {
            'total_actions': 0,
            'clicks': 0,
            'likes': 0,
            'conversions': 0,
            'last_action_time': 0.0,
            'avg_session_duration': 0.0,
            'preferred_categories': set(),
            'device_types': set()
          }

     def update_user_features(self, uid, action_data):
          current_features = self.get(uid) or self.feature_schema.copy()

          good_actions = ['clicks', 'likes', 'conversions']
          
          current_features['total_actions'] += 1
          action_type = action_data.get('action_type', '')
          if action_type in good_actions:
               current_features[action_type] += 1   

          current_features['last_action_time'] = action_data.get('timestamp', time.time())

          #not dealing with device stuff rn

          self.put(uid, current_features)
          return current_features
        

#how we approach this can vary so I put some kinda basic important data storage stuff here and we can vary later...should be easy to modify

class ItemFeatureStore(CuckooHashTable):
    def __init__(self, init_capacity=4096):
         super().__init__(init_capacity)
         self.feature_schema = {
            'total_views': 0,
            'total_likes': 0,
            'total_conversions': 0,
            'categories': set(),
            'avg_rating': 0.0,
            'price_range': '',
            'popularity_score': 0.0,
            'seller_score': 0.0
        }
         
    def update_item_features(self, item_id, action_data):
        current_features = self.get(item_id) or self.feature_schema.copy()

        action_type = action_data.get('action_type', '')
        if action_type == 'likes':
             self.feature_schema['total_likes'] += 1
        elif action_type == 'convert':
             self.feature_schema['total_conversions'] += 1

        self.feature_schema['total_views'] += 1

        #calculate popularity score, this can be changed
        likes = current_features['total_likes']
        views = current_features['total_views']

        current_features['popularity_score'] = likes / views if views > 0 else 0

        self.put(item_id, current_features)
        return current_features
    
class ModelParameterStore(CuckooHashTable):
     
     def __init__(self, init_capacity=512):
          super().__init__(init_capacity)

     def store_weights(self, model_name, weights, metadata=None):
          model_data = {
               'weights': weights,
               'metadata': metadata or {},
               'timestamp': time.time(),
          }
          self.put(model_name, model_data)

     def get_stats(self):
          return self.get_stats()
    
     def get_latest_model(self, model_name):
          return self.get(model_name)
        
        
         


              


              


