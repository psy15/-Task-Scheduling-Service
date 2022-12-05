import json
import pika
from datetime import datetime

# create api to recieve data

jobs = [
    {'job_id': 1, 'priority': 20, 'timestamp': datetime(
        year=2012, month=12, day=2, minute=1, second=12), "dependency": None},
    {'job_id': 2, 'priority': 75, 'timestamp': datetime(
        year=2013, month=12, day=2, minute=1, second=12), "dependency": None},
    {'job_id': 3, 'priority': 40, 'timestamp': datetime(
        year=2015, month=12, day=2, minute=1, second=12), "dependency": None},
    {'job_id': 4, 'priority': 12, 'timestamp': datetime(
        year=2019, month=12, day=2, minute=1, second=12), "dependency": None},
    {'job_id': 5, 'priority': 50, 'timestamp': datetime(
        year=2011, month=12, day=2, minute=1, second=12), "dependency": None},
    {'job_id': 6, 'priority': 12, 'timestamp': datetime(
        year=2009, month=12, day=2, minute=1, second=12), "dependency": None},
]

# job = {'job_id': 1, 'priority': 2, 'timestamp': datetime(
#     year=2012, month=12, day=2, minute=1, second=12), "dependency": None}

# add tasks to rabbitmq
# sender
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queue_name = 'jobs-queue'
channel.queue_declare(queue=queue_name)

# message = 'message#{index}-with-priority {priority}'.format(
#        index=i+1, priority=priority
#        )

for job in jobs:
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(job, default=str)
    )

print("Published:", job)
# connection.close()
