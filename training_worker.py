import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from cuckoo_hash import CuckooHashTable, ModelParameterStore
import redis
import json, time
from kafka import KafkaConsumer
import threading
from collections import deque
import hdfs
import pickle
from models import TrainingExample

class CuckooModelTrainer:

    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.hdfs_client = hdfs.HDFileSystem(host='localhost', port=9000)

        self.scaler = StandardScaler()
        self.model = LogisticRegression(max_iter=1000)

        self.param_store = ModelParameterStore(init_capacity=1024)
        self.training_cache = CuckooHashTable(init_capacity=2048)
        self.feature_cache = CuckooHashTable(init_capacity=4096)

        self.feature_buffer = deque(maxlen=5000)
        self.batch_size = 1000
        self.update_frequency = 100 #updates every _
        self.examples_processed = 0

        #Performance tracking?
        self.stats = {
            'models_trained': 0,
            'examples_processed': 0,
            'training_time': 0,
            'cuckoo_operations': 0,
            'cache_hits': 0,
            'cache_mises': 0
        }

        self.training_threaad = None
        self.should_stop = threading.Event()

        
    def start_background_training(self):
        """Start background training thread"""
        self.training_thread = threading.Thread(target=self._background_training_loop)
        self.training_thread.daemon = True
        self.training_thread.start()
    
    def _background_training_loop(self):
        """Background training loop for continuous model updates"""
        while not self.should_stop.is_set():
            try:
                if len(self.feature_buffer) >= self.batch_size:
                    self._train_model_batch()
                time.sleep(1)  # Prevent busy waiting
            except Exception as e:
                print(f"Error in background training: {e}")
                time.sleep(5)

    def consume_data(self):
        consumer = KafkaConsumer(
            'training-kafka',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='cuckoo-training-worker',
            consumer_timeout_ms=1000
        )

        self.start_background_training()

        for message in consumer:

            try:
                training_ex = TrainingExample(**message.value)

                #cache example
                ex_key = f"{training_ex.user_id}:{training_ex.item_id}:{training_ex.timestamp}"
                self.training_cache.put(ex_key, training_ex)

                self.examples_processed += 1
                self.stats['examples_processed'] += 1

                if self.examples_processed % self.update_frequency == 0:
                    self._log_performance()

            except Exception as e:
                print(f"Error processing example: {e}")
                continue

    def incremental_update(self):
        if len(self.feature_buffer) < 10:
            return
        
        #get most recent data
        recents = self.feature_buffer[-self.update_frequency:]
        features, labels = self._prepare_training_data(recents)

        if len(features) > 0:
            features_scaled = self.scaler.transform(features)

            if hasattr(self.model, 'partial_fit'):
                self.model.partial_fit(features_scaled, labels)
            else:
                self.model.fit(features_scaled, labels)

            self._store_model_parameters('incremental_model')

    def _train_model_batch(self):
        training_start = time.time()

        features, labels = self._prepare_training_data(list(self.feature_buffer))

        if len(features) < 10:
            return
        
        try:
            X = np.array(features)
            y = np.array(labels)

            X_scaled = self.scaler.fit_transform(X)

            self.model.fit(X_scaled, y)

            self._store_model_parameters('batch_model')

            train_duration = time.time() - training_start
            self.stats['training_time'] += train_duration
            self.stats['models_trained'] += 1

            print("Batch model trained")

            #save to Hadoop!
            self._save_data_to_hdfs()

        except Exception as e:
            print(f"Error occurred in batch training: {e}")


    def _prepare_training_data(self, examples):
        features = []
        labels = []

        for example in examples:
            cache_key = f"features:{example.user_id}:{example.item_id}"
            cached_features = self.feature_cache.get(cache_key)

            if cached_features:
                feature_vector = cached_features
                self.stats['cache_hits'] +=1 
            else:
                feature_vector = list(example.features.values())
                self.feature_cache.put(cache_key, feature_vector)
                self.stats['cache_misses'] += 1

            features.append(feature_vector)
            labels.append(example.label)

        return features, labels
    
    def _store_model_parameters(self, model_name):
        try:
            model_weights = {
                'coefficients': self.model.coef_.tolist() if hasattr(self.model, 'coef_') else [],
                'intercept': self.model.intercept_.tolist() if hasattr(self.model, 'intercept_') else 0,
                'classes': self.model.classes_.tolist() if hasattr(self.model, 'classes_') else []
            }
            
            scaler_params = {
                'mean': self.scaler.mean_.tolist() if hasattr(self.scaler, 'mean_') else [],
                'scale': self.scaler.scale_.tolist() if hasattr(self.scaler, 'scale_') else []
            }

            metadata = {}

            #store in our cuckoo table
            self.param_store.store_weights(
                model_name,
                {'model': model_weights, 'scaler': scaler_params},
                metadata
                )
            
            #store in Redis as well
            redis_params = {
                'model': pickle.dumps(self.model),
                'scaler': pickle.dumps(self.scaler),
                'timestamp': time.time(),
                'metadata': json.dumps(metadata)
            }

            self.redis_client.hset(f'model_params:{model_name}', mapping=redis_params)

            self.training_stats['cuckoo_operations'] += 2

        except Exception as e:
            print(f"Error storing parameters: {e}")

    def _save_data_to_hdfs(self):
        try:
            batch_data = []
            for example in list(self.feature_buffer):
                batch_data.append({
                    'user_id': example.user_id,
                    'item_id': example.item_id,
                    'features': example.features,
                    'label': example.label,
                    'timestamp': example.timestamp
                })

                filename = f'/batch_training_data/cuckoo_batch_{int(time.time())}.json'
                with self.hdfs_client.open('filename', 'wb') as f:
                    f.write(json.dumps(batch_data).encode('utf-8'))

        except Exception as e:
            print(f"Error saving to Hadoop: {e}")

    def _log_performance(self):
        cuckoo_stats = self.param_store.get_stats()
        cache_stats = self.feature_cache.get_stats()

        print(f"Training Stats: {self.training_stats}")
        print(f"Model Store: {cuckoo_stats['size']} params, {cuckoo_stats['utilization']} utilization")
        print(f"Feature Cache: {cache_stats['size']} features, {cache_stats['utilization']} utilization")
        print(f"Cache Hit Rate: {self.training_stats['cache_hits'] / (self.training_stats['cache_hits'] + self.training_stats['cache_misses']):.2%}")

        return
    
    def get_model_parameters(self, model_name: str = 'batch_model'):
        return self.model_params_store.get_latest_model(model_name)
    
    def stop(self):
        self.should_stop.set()
        if self.training_thread:
            self.training_thread.join()


if __name__ == "__main":
    trainer = CuckooModelTrainer()
    try:
        trainer.consume_data()
    except KeyboardInterrupt:
        print("Stopping trainer...")
        trainer.stop()
