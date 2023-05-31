import json
import re
from confluent_kafka import Producer

# Configure Kafka connection
bootstrap_servers = 'https://localhost:8083,https://localhost:8083'
topic = 'test'
producer = Producer({'bootstrap_servers': bootstrap_servers',
                    'security.protocol' : 'SSL',
                    'ssl.keystore.password' : 'testSecret',
                    'ssl.keystore.location' : './certkey.pem'
                    })

# Define log file path
log_file = '/Users/xyz/IdeaProjects/src/connect.log'

# Define log pattern using regular expression
log_pattern = r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] (\w+) \[([^\]]+)\] (.*)$'

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
            print(log_data_str)
            # Publish log data to Kafka topic
            producer.produce(topic, value=log_data_str.encode())
#
# # Flush and close the Kafka producer
producer.flush()
producer.close()
