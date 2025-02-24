from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import time

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id and topic_name
project_id = ""
topic_name = "labelRecordsTopic"

# Create a publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Function to convert row values to appropriate types
def convert_row(row):
    return {
        "Timestamp": int(row["Timestamp"]),
        "Car1_Location_X": float(row["Car1_Location_X"]),
        "Car1_Location_Y": int(row["Car1_Location_Y"]),
        "Car1_Location_Z": float(row["Car1_Location_Z"]),
        "Car2_Location_X": float(row["Car2_Location_X"]),
        "Car2_Location_Y": int(row["Car2_Location_Y"]),
        "Car2_Location_Z": float(row["Car2_Location_Z"]),
        "Occluded_Image_view": row["Occluded_Image_view"],
        "Occluding_Car_view": row["Occluding_Car_view"],
        "Ground_Truth_View": row["Ground_Truth_View"],
        "pedestrianLocationX_TopLeft": int(row["pedestrianLocationX_TopLeft"]),
        "pedestrianLocationY_TopLeft": int(row["pedestrianLocationY_TopLeft"]),
        "pedestrianLocationX_BottomRight": int(row["pedestrianLocationX_BottomRight"]),
        "pedestrianLocationY_BottomRight": int(row["pedestrianLocationY_BottomRight"]),
    }

# Read the CSV file and publish messages
csv_file_path = "Labels.csv"
with open(csv_file_path, mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        try:
            formatted_row = convert_row(row)  # Convert values properly
            record_value = json.dumps(formatted_row, ensure_ascii=False).encode("utf-8")

            # Publish message
            future = publisher.publish(topic_path, record_value)
            future.result()  # Ensure successful publishing
            print(f"Published record: {formatted_row}")

        except Exception as e:
            print(f"Failed to publish record: {row}, Error: {e}")

        time.sleep(0.5)  # Optional delay to simulate real-time data
