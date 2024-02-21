import json
import random
import time
import threading
from datetime import datetime
import redis

r = redis.Redis(host="localhost", port=6379, db=0)
# docker run -p 6379:6379 -d redis:latest

def ensure_consumer_group_exists(stream_name, groupname):
    try:
        # implcitly uses id="$"
        r.xgroup_create(name=stream_name, groupname=groupname, mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise


def submit_event(task_key, event_data):
    """
    Submit an event to a stream based on the task_key.
    """
    stream_name = f"stream:{task_key}"
    event_id = r.xadd(stream_name, {"data": json.dumps(event_data)})
    print(f"Event added to {stream_name} with ID {event_id}")


def event_submission_thread():
    """
    Thread function to submit events in random bursts.
    """
    task_keys = ["task1", "task2"]  # Example task keys
    while True:
        sleep_time = random.randint(1, 5)
        print(f"SUBMISSION THREAD: Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

        # Submit a burst of events
        burst_size = random.randint(1, 5)
        for _ in range(burst_size):
            task_key = random.choice(task_keys)
            event_data = {
                "timestamp": str(datetime.now()),
                "detail": f"{task_key} event",
            }
            submit_event(task_key, event_data)


def main_loop(client_name):
    known_streams = [stream.decode() for stream in r.keys("stream:*")]
    while True:
        for stream_name in known_streams:
            ensure_consumer_group_exists(
                stream_name=stream_name, groupname="my_consumer_group"
            )

            # Handling pending messages first
            pending_messages = r.xpending_range(
                stream_name, "my_consumer_group", "-", "+", 10
            )
            if pending_messages:
                message_ids = [msg["message_id"] for msg in pending_messages]
                if message_ids:
                    claimed_messages = r.xclaim(
                        name=stream_name,
                        groupname="my_consumer_group",
                        consumername=client_name,
                        min_idle_time=0,
                        message_ids=message_ids,
                    )
                    # Process claimed messages
                    for _, msg_data in claimed_messages:
                        data_str = msg_data[b"data"].decode("utf-8")
                        data_obj = json.loads(data_str)
                        print(
                            f"Claimed: sending to client from {stream_name}: {data_obj}"
                        )
                    # Acknowledge all claimed messages at once
                    r.xack(stream_name, "my_consumer_group", *message_ids)

            # Reading new messages from the stream using xreadgroup
            messages = r.xreadgroup(
                "my_consumer_group",
                client_name,
                {stream_name: ">"},
                count=5,
                block=1000,
            )
            if new_msg_ids := [msg_id for _, msgs in messages for msg_id, _ in msgs]:
                # Process new messages
                for _, msgs in messages:
                    for msg_id, msg_data in msgs:
                        data_str = msg_data[b"data"].decode("utf-8")
                        data_obj = json.loads(data_str)
                        print(f"Sending to client from {stream_name}: {data_obj}")
                r.xack(stream_name, "my_consumer_group", *new_msg_ids)

        time.sleep(0.1)



def start_simulation():
    submission_thread = threading.Thread(target=event_submission_thread, daemon=True)
    submission_thread.start()

    main_loop("client1")


if __name__ == "__main__":
    try:
        start_simulation()
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)
