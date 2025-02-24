from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob                        # for searching for image files
import base64
import os
import time

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = ""
topic_name = "datasetImagesTopic"  # change it for your topic name if needed

# Create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages with ordering keys to {topic_path}...")

# Directory containing images
image_folder = "Dataset_Occluded_Pedestrian"
image_extension = ["*.png"]
image_files = []

# Search for all image files in the folder
for extension in image_extension:
    image_files.extend(glob.glob(os.path.join(image_folder, extension)))

# Publish each image
for image_file in image_files:
    with open(image_file, "rb") as f:
        # Serialize the image to base64
        value = base64.b64encode(f.read())

    # Use the image name (without the extension) as the message key
    image_name = os.path.splitext(os.path.basename(image_file))[0]
    ordering_key = image_name

    try:
        # Publish the message
        future = publisher.publish(topic_path, value, ordering_key=ordering_key)
        # Ensure successful publishing
        future.result()
        print(f"Published image: {image_name}")

    except Exception as e:
        print(f"Failed to publish image {image_name}. Error: {e}")

    time.sleep(0.5)  # Optional delay between publishing messages
