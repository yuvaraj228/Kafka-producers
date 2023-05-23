from confluent_kafka import Producer
import requests
import json

splunk_hec_url = 'http://<splunk-hec-url>/services/collector'  # Replace with your Splunk HEC URL
splunk_hec_token = 'YOUR_SPLUNK_HEC_TOKEN'  # Replace with your Splunk HEC token
kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers
kafka_topic = 'your-kafka-topic'  # Replace with your Kafka topic

def send_to_kafka(producer, message):
    producer.produce(kafka_topic, value=message)
    producer.flush()

def read_from_splunk_hec():
    response = requests.get(splunk_hec_url, headers={'Authorization': f'Splunk {splunk_hec_token}'})
    if response.status_code == 200:
        return response.json()

def main():
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    while True:
        messages = read_from_splunk_hec()
        if messages:
            for message in messages:
                send_to_kafka(producer, json.dumps(message))

if __name__ == '__main__':
    main()
