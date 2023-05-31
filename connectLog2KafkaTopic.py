import json
import re
from kafka import KafkaProducer

# Configure Kafka connection
bootstrap_servers = 'https://nldkfksql-001.edj.devjones.com:8083'
topic = 'test'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Define log file path
log_file = 'path_to_your_log_file'

# Define log pattern using regular expression
log_pattern = r'^\[([^\]]+)\] \[(DEBUG|INFO|WARN|ERROR)\] \[([^\]]+)\] (.*)$'

# Read log file
with open(log_file, 'r') as file:
    for line in file:
        # Parse log line using the defined pattern
        match = re.match(log_pattern, line)
        if match:
            timestamp, level, component, message = match.groups()

            # Create JSON object
            log_data = {
                'timestamp': timestamp,
                'level': level,
                'component': component,
                'message': message
            }

            # Convert JSON object to string
            log_data_str = json.dumps(log_data)

            # Publish log data to Kafka topic
            producer.send(topic, value=log_data_str.encode())

# Flush and close the Kafka producer
producer.flush()
producer.close()
