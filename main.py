import json
import random
import time
import threading
from datetime import datetime
import redis

from viz import process_messages, update_visuals

# docker run -p 6379:6379 -d redis:latest
r = redis.Redis(host="localhost", port=6379, db=0)

AUTOCLAIM_COUNT = 100
MIN_IDLE_TIME = 10_000

def pprint(msg: str):
    update_visuals(
        thread_name=threading.current_thread().name,
        message=msg
    )

def ensure_consumer_group_exists(stream_name, groupname):
    try:
        # implcitly uses id="$"
        r.xgroup_create(name=stream_name, groupname=groupname, mkstream=True)
        
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise


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


def main_loop(client_name):
    known_streams = [stream.decode() for stream in r.keys("stream:*")]
    while True:
        for stream_name in known_streams:
            ensure_consumer_group_exists(
                stream_name=stream_name, groupname="my_consumer_group"
            )
            start_id = '0-0'  # start from the beginning
            while True:
                result = r.xautoclaim( # https://redis.io/commands/xautoclaim/
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
                
                for _, msg_data in claimed_messages:
                    data_str = msg_data[b"data"].decode("utf-8")
                    data_obj = json.loads(data_str)
                    pprint(f"Claimed and processed: {stream_name}: {data_obj.get('detail')}") # noqa

                if message_ids := [msg_id for msg_id, _ in claimed_messages]:
                    r.xack(stream_name, "my_consumer_group", *message_ids)

                # Prepare for the next iteration if needed
                start_id = next_start_id

            messages = r.xreadgroup(
                groupname="my_consumer_group",
                consumername=client_name,
                streams={stream_name: ">"},  # `>` ~= msgs not seen by consumer group
                count=5, # TODO: how to choose this number?
                block=1000, # TODO: how to choose this number?
            )
            
            # Process new messages and randomly acknowledge most of them
            new_msg_ids = []
            for _, msgs in messages:
                for msg_id, msg_data in msgs:
                    i = msg_id.decode("utf-8")
                    data_str = msg_data[b"data"].decode("utf-8")
                    data_obj = json.loads(data_str)

                    
                    if random.random() < 0.8: # ack 80% of the time
                        pprint(f"Processing + ack for {stream_name}:{i}: {data_obj.get('detail')}") # noqa
                        new_msg_ids.append(i)
                    else:
                        pprint(f"Ope! Not acking {stream_name}:{i}: {data_obj.get('detail')}") # noqa
            
            if new_msg_ids:
                r.xack(stream_name, "my_consumer_group", *new_msg_ids)

        time.sleep(0.1)



def start_simulation():
    client_name = "foo-client"
    threading.Thread(target=process_messages, daemon=True).start()
    submission_thread = threading.Thread(
        target=event_submission_thread,
        daemon=True,
        name=client_name
    )
    submission_thread.start()

    main_loop(client_name)


if __name__ == "__main__":
    try:
        start_simulation()
    except KeyboardInterrupt:
        print("\n\nExiting...")
        exit(0)
