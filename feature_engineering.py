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

class CuckooFeatureExtractor:

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

    def process_element(self):
        return



    