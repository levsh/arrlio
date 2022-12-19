import os
import time

from celery import Celery

app = Celery(
    "tasks",
    broker=f"pyamqp://guest@{os.environ['RABBITMQ_HOST']}//",
    backend="rpc://",
    # backend=f"rpc://guest@{os.environ['RABBITMQ_HOST']}//",
)


@app.task
def hello_world():
    return "Hello World!"


@app.task
def sleep(timeout):
    time.sleep(timeout)
