from google.cloud import pubsub_v1      # pip install google-cloud-pubsub
import glob                             # for searching for json file 
import json
import os 
import csv
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "" # change it for your project id
topic_name = "sensorTopic" # change it for your topic name

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Read the CSV file
csv_file_path = "Labels.csv"
with open(csv_file_path, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Serialize the row into JSON
        record_value = json.dumps(row).encode('utf-8')
        
        try:
            # Publish the message
            future = publisher.publish(topic_path, record_value)
            future.result()  # Ensure successful publishing
            print(f"Published record: {row}")
        except Exception as e:
            print(f"Failed to publish record: {row}, Error: {e}")
        
        time.sleep(0.5)  # Optional delay to simulate real-time data
