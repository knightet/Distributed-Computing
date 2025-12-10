import time

class Process:
    def __init__(self, process_id):
        self.process_id = process_id
        self.clock = 0
        self.message_queue = []

    def tick(self):
        """Increments the local clock for a local event."""
        self.clock += 1
        print(f"Process {self.process_id}: Local event occurred. Clock is now {self.clock}")

    def send_message(self, receiver, message_content):
        """Sends a message to another process."""
        self.clock += 1
        message = {
            'sender_id': self.process_id,
            'timestamp': self.clock,
            'content': message_content
        }
        print(f"Process {self.process_id}: Sending message to {receiver.process_id} with timestamp {self.clock}")
        receiver.receive_message(message)

    def receive_message(self, message):
        """Receives a message and updates the local clock."""
        received_timestamp = message['timestamp']
        self.clock = max(self.clock, received_timestamp) + 1
        self.message_queue.append(message)
        print(f"Process {self.process_id}: Received message from {message['sender_id']}. Updated clock to {self.clock}")


# --- Simulation ---
if __name__ == "__main__":
    # Initialize three processes
    p1 = Process(process_id=1)
    p2 = Process(process_id=2)
    p3 = Process(process_id=3)

    # --- Step-by-step simulation of events ---

    print("--- STEP 1: P1 and P2 have local events ---")
    p1.tick()
    time.sleep(0.5)
    p2.tick()
    time.sleep(0.5)

    print("\n--- STEP 2: P1 sends a message to P2 ---")
    p1.send_message(p2, "Hello from P1")
    time.sleep(0.5)

    print("\n--- STEP 3: P3 has a local event ---")
    p3.tick()
    time.sleep(0.5)

    print("\n--- STEP 4: P2 sends a message to P3 ---")
    p2.send_message(p3, "Hello from P2")
    time.sleep(0.5)

    print("\n--- STEP 5: P3 sends a message to P1 ---")
    p3.send_message(p1, "Hello from P3")
    time.sleep(0.5)
    
    print("\n--- FINAL CLOCK STATES ---")
    print(f"Process 1 Final Clock: {p1.clock}")
    print(f"Process 2 Final Clock: {p2.clock}")
    print(f"Process 3 Final Clock: {p3.clock}")
