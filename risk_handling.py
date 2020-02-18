from infrastructure.mqtt import MQTT

broker = MQTT()
broker.subscribe('emergency/#')