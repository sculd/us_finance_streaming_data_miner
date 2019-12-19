import os
from google.cloud import pubsub_v1

_publisher = None

def _get_topic_name():
    return 'projects/{project_id}/topics/{topic}'.format(
        project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic=os.getenv('FINANCE_STREAM_PUBSUB_ID'),
    )

def _get_publisher():
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher

def publish(msg_str):
    publisher = _get_publisher()
    publisher.publish(_get_topic_name(), msg_str.encode())
