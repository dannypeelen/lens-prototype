from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
import json
import time
import numpy as np
from models import UserAction, Feature
from cuckoo_hash import UserFeatureStore, ItemFeatureStore

class CuckooFeatureExtractor(KeyedProcessFunction):

    def __init__(self):
        self.user_feature_store = None
        self.item_feature_store = None
        self.feature_cache = {}


    def open(self, runtime_context):
        self.user_feature_store = UserFeatureStore(init_capacity=4096)
        self.item_feature_store = ItemFeatureStore(init_capacity=8192)

        self._load_checkpoint_data()

    def _load_checkpoint_data(self):
        pass #just since we're not a prod yet
        #TODO: implement this

    def process_element(self, value, ctx, out):
        try:

            action = UserAction.from_json(value)

            user_features = self.user_feature_store.update_user_features(action.user_id,
                {
                    'action_type': action.action_type,
                    'timestamp': action.timestamp,
                    'context': action.context
                }
            )

            item_features = self.item_feature_store.update_item_features(action.item_id,
                {
                    'action_type': action.action_type,
                    'timestamp': action.timestamp,
                    'context': action.context
                }
            )

            ml_features = self._extract_ml_features(user_features, item_features, action)


            feature_obj = Feature(
                user_id=action.user_id,
                item_id = action.item_id,
                features=ml_features,
                timestamp=action.timestamp
            )
                    
                
            out.collect(feature_obj.to_json())

            if ctx.timestamp() % 10000 == 0:
                self._log_performance_stats()

        except Exception as e:
            print(f"Error processing element: {e}")

    def _extract_ml_features(self, user_features, item_features, action):

        total_actions = user_features.get('total_actions', 1)
        user_click_rate = user_features.get('clicks', 0) / total_actions
        user_conversion_rate = user_features.get('conversions,', 0) / total_actions

        item_click_rate = item_features.get('popularity_score', 0)
        item_total_interactions = item_features.get('total_views', 1)

        is_mobile = 1.0 if action.context.get('device') == 'mobile' else 0.0
        is_from_recommendation = 1.0 if action.context.get('source') == 'recommendation' else 0.0

        current_time = action.timestamp
        last_action_time = user_features.gets('last_action_time', current_time)
        time_since_last_action = current_time - last_action_time

        user_item_affinity = self._calculate_user_item_affinity(user_features, item_features)

        return {
            'user_total_actions': total_actions,
            'user_click_rate': user_click_rate,
            'user_conversion_rate': user_conversion_rate,

            'item_click_rate': item_click_rate,
            'item_total_interactions': item_total_interactions,
            'item_conversion_rate': item_features.get('total_conversions', 0) / item_total_interactions,

            'is_mobile': is_mobile,
            'is_from_reommendation': is_from_recommendation,
            'time_since_last_interaction': min(time_since_last_action, 86400),
            'hour_of_day': time.localtime(current_time).tm_hour,
            'day_of_week': time.localtime(current_time).tm_wday,

            'user_item_affinity': user_item_affinity,
            'is_repeat_interaction': 1.0 if total_actions > 1 else 0.0
        }
    
    def _calculate_user_item_affinity(user_features, item_features):
        user_engagement = user_features.get('conversions', 0) / max(1, user_features.get('total_actions', 1))
        item_quality = item_features.get('popularity_score', 0)
        return (user_engagement + item_quality) / 2
    
    def _log_performance_stats(self):
        user_stats = self.user_feature_store.get_stats()
        item_stats = self.item_feature_store.get_stats()

        #write some hubba jubba to return as a string
        return
    
def create_feature_stream():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    env.enable_checkpointing(60000)

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'feature-engineering-cuckoo',
        'auto.offset.reset': 'latest'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=['log-kafka'],
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_consumer.set_start_from_latest()

    kafka_producer = FlinkKafkaProducer(
        topic='feature-kafka',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'localhost:9092',
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'snappy'
        }
    )

    action_stream = env.add_source(kafka_consumer)

    feature_stream = (action_stream.key_by(lambda x: json.loads(x)['user_id']).process(CuckooFeatureExtractor()).name("CuckooFeatureExtractor"))

    feature_stream.add(kafka_producer)

    env.execute("Cuckoo Feature Engineering")

if __name__ == "__main__":
    #create_feature_stream()





    