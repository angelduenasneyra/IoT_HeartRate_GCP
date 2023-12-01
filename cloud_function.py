import base64
import functions_framework
from google.cloud import firestore
from datetime import datetime, timedelta
import pytz
import paho.mqtt.client as mqtt

# Initialize Firestore client
db = firestore.Client()

# MQTT Broker details
mqtt_broker = "34.27.189.200"
mqtt_username = "msquser"
mqtt_password = "msqpass"
mqtt_port = 1883
mqtt_topic = "topicalert"

def send_mqtt_message(message):
    client = mqtt.Client()
    client.username_pw_set(mqtt_username, mqtt_password)
    client.connect(mqtt_broker, mqtt_port, 60)
    client.publish(mqtt_topic, message)
    client.disconnect()

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Decode the Pub/Sub message
    message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    try:
        # Parse the message
        patient_id, time_string, spo2_str, bpm_str = message.split(', ')
        spo2 = int(spo2_str.split(': ')[1])
        bpm = int(bpm_str.split(': ')[1])

        # Convert the time string to UTC datetime
        naive_time = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
        # Adjust for timezone (UTC-6 in this example)
        tz = pytz.timezone('America/Chicago')
        localized_time = tz.localize(naive_time)
        utc_time = localized_time.astimezone(pytz.utc)

        # Prepare the document to insert
        doc_data = {
            'patientid': patient_id,
            'controltime': utc_time,  # Use the UTC datetime directly
            'bpm': bpm,
            'spo2': spo2
        }

        # Add a new document with a generated ID to the 'heartrate' collection
        db.collection('heartrate').add(doc_data)

        # Calculate average BPM for the last 30 seconds
        thirty_seconds_ago = utc_time - timedelta(seconds=30)
        records = db.collection('heartrate').where('patientid', '==', patient_id).where('controltime', '>=', thirty_seconds_ago).stream()
        total_bpm, count = 0, 0
        for record in records:
            total_bpm += record.to_dict()['bpm']
            count += 1
        if count > 0:
            avg_bpm = round(total_bpm / count)

            # Fetch threshold values
            # Fetch threshold values
            thresholds = db.collection('threshold').limit(1).stream()
            for threshold in thresholds:
                threshold_data = threshold.to_dict()
                max_bpm = threshold_data['maxbpm']
                min_bpm = threshold_data['minbpm']
                # you can also fetch maxspo2 and minspo2 if needed
                break  # Since there's only one document, we break after the first iteration


            # Check if average BPM is outside the threshold
            if avg_bpm > max_bpm or avg_bpm < min_bpm:
                # Fetch patient's name
                #patient = db.collection('patient').document(patient_id).get().to_dict()
                #patient = db.collection('patient').where('patientid', '==', patient_id).limit(1).stream()
                #patient_name = f"{patient['firstname']} {patient['lastname']}"

                patient_query = db.collection('patient').where('patientid', '==', patient_id).limit(1).stream()
                patient_name = "Unknown"
                for patient in patient_query:
                    patient_data = patient.to_dict()
                    patient_name = f"{patient_data['firstname']} {patient_data['lastname']}"
                    break 

                # Prepare and send MQTT message
                alert_message = f"Alert, {time_string}, patientid: {patient_id}, patientname: {patient_name}, bpm: {avg_bpm}, spo2: {spo2}"
                send_mqtt_message(alert_message)

    except Exception as e:
        print(f"Error processing message: {message}\n{e}")

    # Optional: Print the data to the logs
    print("Processed message:", message)
