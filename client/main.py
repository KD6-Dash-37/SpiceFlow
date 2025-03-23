import zmq

from spiceflow_client.order_book import deserialise_and_print


def main():
    
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5556")
    # Subscribe to all topics
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("Listening for messages...")
    
    while True:
        try:
            
            # Receive the message in raw bytes
            print(socket.recv_string())
            buf: bytearray = socket.recv()
            deserialise_and_print(buf)

            
        
        except KeyboardInterrupt:
            print("Shutting down client.")
            break
    socket.close()
    context.term()

if __name__ == "__main__":
    main()
