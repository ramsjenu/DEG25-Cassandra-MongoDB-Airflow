from confluent_kafka.admin import AdminClient, NewTopic, KafkaException, KafkaError
import time
import logging

logger = logging.getLogger(__name__)

def kafka_create_topic_main(topic_name='email_topic', num_partitions=1, replication_factor=1, max_retries=5, retry_delay=5):
    admin_client = AdminClient({'bootstrap.servers': 'bitnami-kafka:9092'})
    
    for attempt in range(1, max_retries + 1):
        try:
            metadata = admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' already exists")
                return "topic_already_exists"

            # Topic does not exist, attempt creation
            new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            fs = admin_client.create_topics([new_topic])

            # Wait for result
            f = fs[topic_name]
            f.result(timeout=30)
            logger.info(f"Topic '{topic_name}' created successfully")
            return "topic_created"

        except KafkaException as e:
            # Topic already exists (sometimes concurrent race condition)
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info(f"Topic '{topic_name}' already exists (caught exception)")
                return "topic_already_exists"
            else:
                logger.warning(f"Attempt {attempt}: KafkaException: {e}. Retrying in {retry_delay}s...")
        except Exception as e:
            logger.warning(f"Attempt {attempt}: Unexpected exception: {e}. Retrying in {retry_delay}s...")

        time.sleep(retry_delay)

    logger.error(f"Failed to create topic '{topic_name}' after {max_retries} attempts")
    return "topic_failed"
