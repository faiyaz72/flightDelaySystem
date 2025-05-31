import pika
import json
import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

# RabbitMQ connection parameters
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

# 1. Establish connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)))
channel = connection.channel()

# 2. Declare the queue (idempotent)
channel.queue_declare(queue=RABBITMQ_QUEUE)

# 3. Simulate a flight delay event
message = {
    "flightId": "AC101",
    "status": "DELAYED",
    "newDepartureTime": (datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat(),
    "reason": "Weather - low visibility"
}

# 4. Send message
channel.basic_publish(
    exchange='',
    routing_key=RABBITMQ_QUEUE,
    body=json.dumps(message)
)

print(f"✅ Sent message: {message}")
connection.close()# Publishes delay events
