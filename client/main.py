import zmq
import httpx
from spiceflow_client.order_book import deserialise_and_print

def subscribe_to_stream() -> None:
    url = "http://localhost:3000/subscribe"
    payload = {
        "base": "BTC",
        "quote": "USD",
        "instrument_type": "InvPerp",
        "exchange": "Deribit",
        "requested_feed": "OrderBook"
    }
    try:
        response = httpx.post(url, json=payload)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP error: {e.response.status_code} - {e.response.text}")
        return
    except Exception as e:
        print(f"❌ Failed to send subscribe request: {e}")
        return
    print("✅ Successfully subscribed.")


def unsubscribe_to_stream() -> None:
    url = "http://localhost:3000/unsubscribe"
    payload = {
        "base": "BTC",
        "quote": "USD",
        "instrument_type": "InvPerp",
        "exchange": "Deribit",
        "requested_feed": "OrderBook"
    }
    try:
        response = httpx.post(url, json=payload)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP error: {e.response.status_code} - {e.response.text}")
        return
    except Exception as e:
        print(f"❌ Failed to send unsubscribe request: {e}")
        return
    print("✅ Successfully unsubscribed.")


def main():
    subscribe_to_stream()
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
            unsubscribe_to_stream()
            print("🛑 Shutting down client.")
            break
    socket.close()
    context.term()

if __name__ == "__main__":
    main()
