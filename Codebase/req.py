from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def check_kafka_producer(bootstrap_servers='localhost:9092', topic='test_topic'):
    try:
       
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version = (1,3,5)
        )
        message = {"message": "Hello, Kafka!"}
        future = producer.send(topic, value=message)
        result = future.get(timeout=10)
        print(f"✅ Kafka Producer: Message sent successfully! Result: {result}")

        # Close the producer
        producer.close()
    except KafkaError as e:
        print(f"❌ Kafka Producer Error: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    check_kafka_producer()
