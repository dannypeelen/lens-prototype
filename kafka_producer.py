import json, time, random
from kafka import KafkaProducer
from models import UserAction

class UserActionProducer:

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )

    """not real """
    def simulate_user_actions(self, duration_seconds=3600):
        """Simulate User Actions"""

        users = [f"user_{i}" for i in range(1, 101)]
        items = [f"item_{i}" for i in range(1, 1001)]
        actions = ["like", "dislike", "cart"]

        start_time = time.time()

        while time.time() - start_time < duration_seconds:
            action = UserAction(
                user_id=random.choice(users),
                item_id=random.choice(items),
                action_type=random.choice(actions),
                timestamp=time.time(),
                context={
                    "device": random.choice(["mobile", "desktop", "tablet"]), #idk if we want device context? prolly not
                    "source": random.choice(["search", "recommendation", "browse"])
                }
            )

            self.producer.send(
                'log-kafka',
                key=action.user_id,
                value=action.__dict__
            )

            time.sleep(random.uniform(0.1, 2.0))

        self.producer.flush()
        self.produce.close()

if __name__ == "__main__":
    producer = UserActionProducer()
    #simulate here if ya want
