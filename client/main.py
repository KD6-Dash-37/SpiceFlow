import zmq

def main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    print("Listening for messages...")
    while True:
        try:
            topic = socket.recv_string()
            message = socket.recv()
            print(f"Topic: {topic}")
            print(f"Message (raw bytes): {message}")
        except KeyboardInterrupt:
            print("Shutting down client.")
            break
    socket.close()
    context.term()

if __name__ == "__main__":
    main()
