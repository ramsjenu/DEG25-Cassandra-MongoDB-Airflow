from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection_name = collection_name

    def create_collection(self):
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)
            logger.info(f"Created collection: {self.collection_name}")
        else:
            logger.info(f"Collection {self.collection_name} already exists")

    def insert_data(self, email, otp):
        document = {
            "email": email,
            "otp": otp
        }
        self.db[self.collection_name].insert_one(document)
        logger.info(f"Inserted document: {document}")

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed")

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics, mongodb_connector):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe(topics)
        self.mongodb_connector = mongodb_connector

    def consume_and_insert_messages(self, run_duration_secs=None):
        start_time = time.time()
        try:
            while True:
                if run_duration_secs and (time.time() - start_time) >= run_duration_secs:
                    logger.info("Reached run duration, stopping consumer")
                    break

                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                    else:
                        logger.warning(f"Kafka error: {msg.error()}")
                    continue

                # Decode Kafka message
                email = msg.key().decode('utf-8')
                otp = msg.value().decode('utf-8')

                # Check if email already exists
                existing_document = self.mongodb_connector.db[self.mongodb_connector.collection_name].find_one({"email": email})
                if existing_document:
                    logger.warning(f"Document with Email={email} already exists, skipping insert")
                else:
                    self.mongodb_connector.insert_data(email, otp)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping consumer")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        self.mongodb_connector.close()
        logger.info("Kafka consumer closed")

def kafka_consumer_mongodb_main():
    # MongoDB config
    mongodb_uri = 'mongodb://vrams:vinu2003@mongodb:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'

    # Initialize MongoDB
    mongodb_connector = MongoDBConnector(mongodb_uri, database_name, collection_name)
    mongodb_connector.create_collection()

    # Kafka consumer config
    kafka_config = {
        'bootstrap.servers': 'bitnami-kafka:9092',
        'group.id': 'mongo_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    topics = ['email_topic']

    # Start Kafka consumer
    kafka_consumer = KafkaConsumerWrapperMongoDB(kafka_config, topics, mongodb_connector)
    # Set run_duration_secs=None for continuous run, or e.g., 30 seconds
    kafka_consumer.consume_and_insert_messages(run_duration_secs=30)

if __name__ == '__main__':
    kafka_consumer_mongodb_main()
