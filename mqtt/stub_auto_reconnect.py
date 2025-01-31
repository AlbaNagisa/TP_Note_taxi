import os
import random
import time
from datetime import datetime, timezone
import json
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTErrorCode


MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = os.getenv("MQTT_TOPIC")


LICENSE_PLATES = ["AA-123-BB", "CC-456-DD", "EE-789-FF", "GG-012-HH", "II-345-JJ", "KK-678-LL", "MM-901-NN", "OO-234-PP", "QQ-567-RR", "SS-890-TT", "UU-123-VV", "WW-456-XX", "YY-123-XX", "AA-477-CC", "CC-985-JI", "FE-619-FE", "GG-345-HH", "II-678-JJ", "DM-843-LM", "MM-456-NN", "OO-789-PP", "QQ-012-RR", "SS-345-TT", "UU-678-VV", "WW-901-XX", "YY-123-ZZ", "AA-456-BB", "CC-789-DD", "EE-012-FF", "FF-345-GG", "GG-678-HH", "HH-901-II", "II-234-JJ", "KK-123-LL", "LL-456-MM", "NN-789-OO", "PP-012-QQ", "RR-345-SS", "TT-678-UU", "VV-901-WW", "XX-234-YY", "ZZ-567-AA", "BB-890-CC"]

TAXIS = [
    {"license_plate": f"{plate_number}", "lat": random.uniform(48.85, 48.90), "lon": random.uniform(2.33, 2.40), "moving": True}
    for plate_number in LICENSE_PLATES
]

def simulate_taxi_movement(taxi):
    if taxi["moving"]:
        taxi["lat"] += random.uniform(-0.0005, 0.0005)
        taxi["lon"] += random.uniform(-0.0005, 0.0005)
        if random.random() < 0.1:
            taxi["moving"] = False
    else:
        if random.random() < 0.2:
            taxi["moving"] = True

    return {
        "license_plate": taxi["license_plate"],
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "lat": round(taxi["lat"], 6),
        "lon": round(taxi["lon"], 6),
        "status": "moving" if taxi["moving"] else "stopped"
    }


def publish_gps_data(client):
    success = True
    for taxi in TAXIS:
        data = simulate_taxi_movement(taxi)
        message = json.dumps(data)
        response = client.publish(MQTT_TOPIC, message)
        if response.rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
            print(f"Error for message {message}")
            success = False
            break
        else:
            print(f"Success for message {message}")

    return success


def set_connection(client):
    client.disconnect()
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        print(f"Connected to this MQTTT : ({MQTT_BROKER}:{MQTT_PORT})")
    except Exception as e:
        print(f"Error occurred while connecting to MQTT : {e}")
        return
            

def main():
    client = mqtt.Client()
    set_connection(client)
    try:
        while True:
            if not publish_gps_data(client):
                set_connection(client)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nDisconnexion from user input.")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
