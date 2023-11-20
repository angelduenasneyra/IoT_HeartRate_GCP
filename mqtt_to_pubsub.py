import paho.mqtt.client as mqtt
from google.cloud import pubsub_v1

# Configuration
mqtt_broker = "localhost"
mqtt_port = 1883
mqtt_topic = "topictest"
mqtt_username = "msquser"  # Replace with your MQTT username
mqtt_password = "msqpass"  # Replace with your MQTT password
gcp_project_id = "pocbigquery-398813"
pubsub_topic = "pubsubheartrate"

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(gcp_project_id, pubsub_topic)

# Callback when connected to MQTT broker
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(mqtt_topic)

# Callback when a message is received from MQTT broker
def on_message(client, userdata, msg):
    print(f"Received message '{msg.payload.decode()}' from topic '{msg.topic}'")
    future = publisher.publish(topic_path, msg.payload)
    future.add_done_callback(callback)

def callback(future):
    try:
        # This will raise an exception if the publish failed.
        print(f"Message published: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Set up MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Set username and password
mqtt_client.username_pw_set(mqtt_username, mqtt_password)

# Connect to MQTT broker
mqtt_client.connect(mqtt_broker, mqtt_port, 60)

# Start the loop
mqtt_client.loop_forever()
