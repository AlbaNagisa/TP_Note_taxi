import json
import os
import sys
from datetime import datetime
sys.path.append('./')
from database import Database
import paho.mqtt.client as mqtt


db = Database()
db.connect()
db.set_database('Alban_Girardi_Taxis')
db[os.getenv('COLLECTION_NAME')].count_documents({})

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    # Since we subscribed only for a single channel, reason_code_list contains
    # a single entry
    if reason_code_list[0].is_failure:
        print(f"Broker rejected you subscription: {reason_code_list[0]}")
    else:
        print(f"Broker granted the following QoS: {reason_code_list[0].value}")

def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
    # Be careful, the reason_code_list is only present in MQTTv5.
    # In MQTTv3 it will always be empty
    if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
        print("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
    else:
        print(f"Broker replied with failure: {reason_code_list[0]}")
    client.disconnect()

def on_message(client, userdata, message):
    json_data = json.loads(message.payload.decode("utf-8"))
    if not all(key in json_data for key in ["license_plate", "lat", "lon", "status", "timestamp"]):
        print("Invalid message received")
    json_data['timestamp'] = datetime.fromisoformat(json_data['timestamp'][:-6])
    db.insert_one("location", json_data)



def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        # we should always subscribe from on_connect callback to be sure
        # our subscribed is persisted across reconnections.
        client.subscribe(os.getenv("MQTT_TOPIC"))

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.on_subscribe = on_subscribe
mqttc.on_unsubscribe = on_unsubscribe

mqttc.user_data_set([])
mqttc.connect("broker.hivemq.com", 1883)
mqttc.loop_forever()
