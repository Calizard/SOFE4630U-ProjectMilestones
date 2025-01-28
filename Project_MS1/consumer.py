from google.cloud import pubsub_v1      # pip install google-cloud-pubsub
import glob                             # for searching for json file 
import json
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with project ID
project_id = "" # change it for your project id
topic_name = "sensorTopic" # change it for your topic name
subscription_id = "sensorTopic-sub" # change it for your topic's subscription id

# Create a subscriber and get the subscription path
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Deserialize the message data
        message_data = json.loads(message.data.decode('utf-8'))
        print(f"Consumed record: {message_data}")
        
        # Acknowledge the message after successful processing
        message.ack()
    except Exception as e:
        print(f"Failed to process message: {e}")

# Start the subscriber
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
