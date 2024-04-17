import concurrent.futures
import queue
import time
import threading


class MyQueue:
    def __init__(self):
        self.q = queue.Queue()
        self.running = True

    def add_to_queue(self, item):
        self.q.put(item)

    def process_queue(self):
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     executor.map(self.run, iter(self.q.get, None))
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            while self.running or not self.q.empty():
                try:
                    item = self.q.get(block=False)
                    executor.submit(self.run, item)
                except queue.Empty:
                    time.sleep(0.1)  # Avoid busy waiting

    def run(self, item):
        # Perform some processing on the item
        print(f"Processing item: {item}")
        time.sleep(2)  # Simulate processing time

    def stop(self):
        self.running = False


# Usage example
my_queue = MyQueue()

# Start processing the queue in a separate thread
threading.Thread(target=my_queue.process_queue, daemon=True).start()

# Add items to the queue
my_queue.add_to_queue("item1")
my_queue.add_to_queue("item2")
my_queue.add_to_queue("item3")
my_queue.add_to_queue("item4")
my_queue.add_to_queue("item5")

# Simulate an external process adding an item to the queue
time.sleep(30)
my_queue.add_to_queue("item6")
my_queue.add_to_queue("item7")
my_queue.add_to_queue("item8")
my_queue.add_to_queue("item9")
my_queue.add_to_queue("item10")
my_queue.add_to_queue("item11")

while my_queue.running or not my_queue.q.empty():
    print("sleeping")
    time.sleep(1)

my_queue.stop()
