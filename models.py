from dataclasses import dataclass
from typing import Dict, List, Optional
import json


@dataclass
class UserAction:
    user_id:str
    item_id:str
    action_type:str
    timestamp:float
    context:Dict

    def to_json(self):
        return json.dumps(self.__dict__)
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)
    
@dataclass
class Feature:
    user_id:str
    item_id:str
    features: Dict[str, float]
    timestamp: float

    def to_json(self):
        return json.dumps(self.__dict__)
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)
    
@dataclass
class TrainingExample:

    def __init__(self, model=""):
        self.model = model

    def to_json(self):
        return json.dumps(self.__dict__)