import json
import random
import time
import threading
from datetime import datetime
from functools import partial
import redis

from viz import process_messages, update_visuals

# docker run -p 6379:6379 -d redis:latest
r = redis.Redis(host="localhost", port=6379, db=0)


SAMPLE_CLIENT_NAME = "foo-client"
MIN_IDLE_TIME = 10_000  # 10 seconds
AUTOCLAIM_COUNT = 100
READ_COUNT = 5
READ_BLOCK = 1000  # 1 second

def pprint(msg: str):
    update_visuals(
        thread_name=threading.current_thread().name,
        message=msg
    )

# EVENT SUBMISSION
def submit_event(task_key, event_data):
    stream_name = f"stream:{task_key}"
    event_id = r.xadd(stream_name, {"data": json.dumps(event_data)}).decode("utf-8")
    pprint(f"submitting entry ... {stream_name}:{event_id}")


def event_submission_thread():
    """
    Thread function to submit events in random bursts.
    """
    task_keys = ["task1", "task2"]
    
    while True:
        sleep_time = random.randint(1, 5)
        pprint(f"SUBMISSION THREAD: Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

        # Submit a burst of events
        for _ in range(random.randint(1, 5)):
            task_key = random.choice(task_keys)
            event_data = {
                "timestamp": str(datetime.now()),
                "detail": f"some event for {task_key}",
            }
            submit_event(task_key, event_data)

# CONSUMER GROUP
def ensure_consumer_group(stream_name: str, groupname: str):
    try:
        # implcitly uses id="$"
        r.xgroup_create(name=stream_name, groupname=groupname, mkstream=True)
        
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise

def process_claimed_messages(stream_name: str, claimed_messages: list):
    for _, msg_data in claimed_messages:
        data_str = msg_data[b"data"].decode("utf-8")
        data_obj = json.loads(data_str)
        pprint(f"Claimed and processed: {stream_name}: {data_obj.get('detail')}")
    
    return [msg_id for msg_id, _ in claimed_messages]

def process_new_messages(stream_name: str, consumer_name: str):
    new_msg_ids = []
    messages = r.xreadgroup(
        groupname="my_consumer_group",
        consumername=consumer_name,
        streams={stream_name: ">"},
        count=READ_COUNT,
        block=READ_BLOCK,
    )

    for _, msgs in messages:
        for msg_id, msg_data in msgs:
            i = msg_id.decode("utf-8")
            data_str = msg_data[b"data"].decode("utf-8")
            data_obj = json.loads(data_str)

            if random.random() < 0.8:  # Ack 80% of the time
                pprint(f"Processing + ack for {stream_name}:{i}: {data_obj.get('detail')}") # noqa
                new_msg_ids.append(i)
            else:
                pprint(f"Ope! Not acking {stream_name}:{i}: {data_obj.get('detail')}")

    return new_msg_ids

def acknowledge_messages(stream_name: str, group_name: str, message_ids: list):
    if message_ids:
        r.xack(stream_name, group_name, *message_ids)

# MAIN LOOP
def main_loop(client_name):
    known_streams = [stream.decode() for stream in r.keys("stream:*")]

    while True:
        for stream_name in known_streams:
            ensure_consumer_group(stream_name, "my_consumer_group")

            start_id = '0-0'  # Start from the beginning
            while True:
                result = r.xautoclaim(
                    name=stream_name,
                    groupname="my_consumer_group",
                    consumername=client_name,
                    min_idle_time=MIN_IDLE_TIME,
                    start_id=start_id,
                    count=AUTOCLAIM_COUNT,
                )
                next_start_id, claimed_messages = result[0], result[1]

                if not claimed_messages:
                    break

                message_ids = process_claimed_messages(stream_name, claimed_messages)
                acknowledge_messages(stream_name, "my_consumer_group", message_ids)
                start_id = next_start_id

            new_msg_ids = process_new_messages(stream_name, client_name)
            acknowledge_messages(stream_name, "my_consumer_group", new_msg_ids)

            time.sleep(0.1)


def start_simulation(client_name: str):
    threading.Thread(
        target=partial(process_messages, client_name),
        daemon=True
    ).start()
    threading.Thread(
        target=event_submission_thread,
        daemon=True,
        name=client_name
    ).start()

    main_loop(client_name)


if __name__ == "__main__":
    try:
        start_simulation(SAMPLE_CLIENT_NAME)
    except KeyboardInterrupt:
        print("\n\nExiting...")
        exit(0)
