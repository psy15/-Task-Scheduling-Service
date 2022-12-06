"""The Flask App."""

# pylint: disable=broad-except

from jobs import Job
from pydantic import ValidationError
from flask import Flask, abort, jsonify, request
import json
import pika
from datetime import datetime
from rq.job import Job

# from redis_resc import redis_conn, redis_queue

app = Flask(__name__)


@app.errorhandler(404)
def resource_not_found(exception):
    """Returns exceptions as part of a json."""
    return jsonify(error=str(exception)), 404


@app.route("/")
def home():
    """Show the app is working."""
    return "Running!"


@app.route("/enqueue", methods=["POST"])
def enqueue():
    """Enqueues a task into redis queue to be processes.
    Returns the job_id."""

    if request.method == "POST":
        data = request.json

        data['timestamp'] = datetime.now()

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        channel = connection.channel()

        queue_name = 'jobs-queue'
        channel.queue_declare(queue=queue_name)

        # push data to rabbitmq
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(data, default=str)
        )

    return jsonify({"job_id": data["job_id"]})


@app.route("/get_result")
def get_result():
    """Takes a job_id and returns the job's result."""
    job_id = request.args["job_id"]

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception as exception:
        abort(404, description=exception)

    if not job.result:
        abort(
            404,
            description=f"No result found for job_id {job.id}. Try checking the job's status.",
        )
    return jsonify(job.result)


if __name__ == "__main__":
    app.run(debug=True)


# rq worker scheduled-jobs --with-scheduler --path C:\Users\paras\Documents\code\ringover\priority-queue
