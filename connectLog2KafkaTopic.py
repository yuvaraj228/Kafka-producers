import json
import re
from confluent_kafka import Producer

def send_to_kafka(producer, topic, message):
    # Convert JSON object to string
    message_str = json.dumps(message)
    try:
        producer.produce(topic, value=message_str.encode('utf-8'))
        producer.flush()
        print(f"Message sent to Kafka topic: {topic}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")
      
def replace_newlines(input_string):
    replaced_string = input_string.replace('\n', ' ')
    return replaced_string
    
def reg_match(match):
    timestamp, level, component, message = match.groups()

    # Create JSON object
    log_data = {
        'timestamp': timestamp,
        'level': level,
        'component': component,
        'message': message
    }
    return log_data
  
def parse_msg():
  try:
  line = replace_newlines(line)
  except:
      print('Nothing to replace')
  else:
      match = re.match(log_pattern, line)
      if match:
          return reg_match(match)
      else:
          print('else')
        
def main():
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
  try:
      # Use tail to continuously monitor the log file
      with subprocess.Popen(['tail', '-n', '0', '-F', log_file], stdout=subprocess.PIPE, universal_newlines=True) as process:
          for line in process.stdout:
              # For multiline log messages, we need to read until we find an empty line or a new log line
              message = line
              while True:
                  next_line = process.stdout.readline()
                  if not next_line.strip() or next_line.startswith(" "):
                      message += next_line
                  else:
                      break
              parsed_msg = parse_msg(message)

              # Send the message to Kafka
              send_to_kafka(producer, topic, parsed_msg)

  except KeyboardInterrupt:
      print("Interrupted. Closing the producer.")
      producer.flush()
      producer.close()

if __name__ == "__main__":
    main()
