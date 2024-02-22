from queue import Queue
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout

console = Console()
layout = Layout()

N_MESSAGES = 50

layout.split_row(
    Layout(name="main_thread"),
    Layout(name="submission_thread")
)
layout["main_thread"].update(Panel("", title="Main Thread Log"))
layout["submission_thread"].update(Panel("", title="Submission Thread Log"))

queue = Queue()

def process_messages(logs, client_name):
    while True:
        thread_name, message = queue.get()
        
        if len(logs[thread_name]) >= N_MESSAGES:
            logs[thread_name].pop(0)
        logs[thread_name].append(message)
        
        layout["main_thread"].update(
            Panel("\n".join(logs["MainThread"]), title="Main Thread Log")
        )
        layout["submission_thread"].update(
            Panel("\n".join(logs[client_name]), title="Client Thread Log")
        )

        console.clear()
        console.print(layout, end="/r")
        
        queue.task_done()


def update_visuals(thread_name, message):
    queue.put((thread_name, message))
