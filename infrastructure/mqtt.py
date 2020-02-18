import json
import os
import time
import logging
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

import constant
from risk.module.util import *
from task.module.consumer import update_job_info


class MQTT:
    def __init__(self):
        self.client = None
        self.connect()

    def connect(self):
        load_dotenv()

        def on_connect(client, userdata, flags, rc):
            print("Connected with result code " + str(rc))

        def on_message(client, userdata, msg):
            print(msg.topic + " " + str(msg.payload))

        def on_publish(client, obj, mid):
            print("mid: " + str(mid))

        def on_subscribe(client, obj, mid, granted_qos):
            print("Subscribed: " + str(mid) + " " + str(granted_qos))

        def on_log(client, obj, level, string):
            print(string)

        def on_disconnect(client, userdata, rc):
            print(rc)
            if rc != 0:
                print("Unexpected disconnection.")
            while True:
                client.reconnect()
                time.sleep(5)

        def on_message_emergency_account(client, userdata, msg):
            print(msg.topic + " " + str(msg.payload))
            message = json.loads(msg.payload)
            switch_account(message)

        def on_message_emergency_following(client, userdata, msg):
            print(msg.topic + " " + str(msg.payload))
            message = json.loads(msg.payload)
            disable_following(message)

        def on_message_job_update(client, userdata, msg):
            print(msg.topic + " " + str(msg.payload))
            message = json.loads(msg.payload)
            update_job_info(message)

        self.client = mqtt.Client()

        self.client.on_connect = on_connect
        self.client.on_message = on_message
        self.client.on_publish = on_publish
        self.client.on_subscribe = on_subscribe
        self.client.on_log = on_log

        self.client.message_callback_add(constant.MQTT_TOPIC_EMERGENCY_ACCOUNT, on_message_emergency_account)
        self.client.message_callback_add(constant.MQTT_TOPIC_EMERGENCY_FOLLOWING, on_message_emergency_following)
        self.client.message_callback_add(constant.MQTT_TOPIC_JOB_UPDATE, on_message_job_update)

        try:
            rc = self.client.connect(host=os.getenv('MQTT_HOST'), port=int(os.getenv('MQTT_PORT')))
            print(rc)
        except Exception as ex:
            print(ex)
            time.sleep(5)
            logging.info("Reconnect to MQTT Server ...")
            rc = self.connect()
        return rc

    def send_message(self, topic, content):
        try:
            message_json = content.to_json()
            self.client.publish(topic, message_json)
        except Exception as exc:
            print(exc)
            rc = self.connect()
            while rc == 0:
                print('Reconnect to MQTT Server ...')
                rc = self.connect()
                if rc == 1:
                    break
                time.sleep(5)

            self.send_message(topic, content)

    def subscribe(self, topic):
        self.client.subscribe(topic)
        self.client.loop_forever()
