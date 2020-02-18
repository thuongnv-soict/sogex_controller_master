import json
import logging
import os

from dotenv import load_dotenv
import constant
import pika

from task.module.consumer import update_job_info


class RabbitMQ:
    def __init__(self):
        self.channel = None
        self.connection = None
        self.properties = pika.BasicProperties(delivery_mode=constant.RABBITMQ_DELIVERY_MODE)
        self.connect()

    def connect(self):
        load_dotenv()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST'), port=os.getenv('RABBITMQ_PORT')))
        logging.info("Connected to RabbitMQ Server")
        print("Connected to RabbitMQ Server")

        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=constant.RABBITMQ_EXCHANGE_NAME,
                                      exchange_type=constant.RABBITMQ_EXCHANGE_TYPE)
        logging.info("Declared exchange successfully")

        # Declare 'task_queue'
        self.channel.queue_declare(queue=constant.RABBITMQ_MONITOR_QUEUE, durable=constant.RABBITMQ_QUEUE_DURABLE)
        self.channel.queue_bind(exchange=constant.RABBITMQ_EXCHANGE_NAME,
                                queue=constant.RABBITMQ_MONITOR_QUEUE,
                                routing_key=constant.RABBITMQ_MONITOR_QUEUE)
        logging.info("Declared queue " + constant.RABBITMQ_MONITOR_QUEUE + " successfully")

    def send_message(self, routing_key, data):
        message = data.to_json()
        try:
            self.channel.basic_publish(
                exchange=constant.RABBITMQ_EXCHANGE_NAME,
                routing_key=routing_key,
                body=message,
                # properties=self.properties
            )
        except Exception as exc:
            print(exc)
            print('Reconnect to RabbitMQ')
            self.connect()
            self.send_message(routing_key, data)
            print("Send message successfully")

    def start_consuming_message(self):
        def callback(ch, method, properties, body):
            message = json.loads(body)
            logging.info(" [x] Received %r" % body)
            update_job_info(message)
            print(message)
            self.channel.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=constant.RABBITMQ_MONITOR_QUEUE, on_message_callback=callback)
        self.channel.start_consuming()

