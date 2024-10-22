from dotenv import load_dotenv
import os
from atproto import Client

import json
from time import sleep
from concurrent import futures
from google.cloud import pubsub_v1

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Max messages to gcp pubsub
MAX_MESSAGES = 1000

#  Configure the batch to publish as soon as there are 10 messages
# or 1 KiB of data, or 1 second has passed.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=MAX_MESSAGES,  # default 100
    max_bytes=1024,  # default 1 MB
    max_latency=10,  # default 10 ms
)

# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    print(message_id)

def extract_data(max_results):
    
    project_id = os.environ["PROJECT_ID"]
    topic_name = os.environ["TOPIC_ID"]

    publish_client = pubsub_v1.PublisherClient()
    topic_path = F'projects/{project_id}/topics/{topic_name}'
    publish_futures  = []

    client = Client()

    user_name = os.getenv("USERNAME")
    password = os.getenv("KEY")

    client.login(user_name, password)

    data = client.app.bsky.feed.get_feed({
        'feed': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot',
        'limit': 10,
    })

    feed = data.feed

    # Suponiendo que el objeto que proporcionaste está guardado en una variable llamada feed_posts
    for feed_post in feed:
        bsky_json = {}
        bsky_json["id"] = feed_post.post.cid
        bsky_json["text"] = feed_post.post.record.text if feed_post.post.record.text else "No hay texto"
        publish_future = publish_client.publish(topic_path, data=json.dumps(bsky_json).encode("utf-8"))
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)

    res = futures.wait(publish_futures , return_when=futures.ALL_COMPLETED)

    return res



if __name__ == '__main__':
    while True:
        tweets_json_batch = extract_data(max_results=MAX_MESSAGES)
        sleep(5)