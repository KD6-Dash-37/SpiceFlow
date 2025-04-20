# client/main.py

import argparse

import httpx
import zmq

from spiceflow_client.order_book import deserialise_and_print


PAYLOADS: dict[str, dict] = {
    "deribit": {
        "base": "BTC",
        "quote": "USD",
        "instrument_type": "InvPerp",
        "exchange": "Deribit",
        "requested_feed": "OrderBook"
    },
    "binance": {
        "base": "BTC",
        "quote": "USDT",
        "instrument_type": "Spot",
        "exchange": "Binance",
        "requested_feed": "OrderBook"
    }
}

URL = "tcp://localhost:5556"


def subscribe_to_stream(payload: dict) -> None:
    url = "http://localhost:3000/subscribe"
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


def unsubscribe_to_stream(payload: dict) -> None:
    url = "http://localhost:3000/unsubscribe"
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
    parser = argparse.ArgumentParser(
        description="Subscribe to exchange feed"
    )
    parser.add_argument(
        "-p",
        "--payload",
        choices=PAYLOADS.keys(),
        required=True
    )
    args = parser.parse_args()
    selected_payload = PAYLOADS[args.payload]
    
    subscribe_to_stream(selected_payload)
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(URL)
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
            unsubscribe_to_stream(selected_payload)
            print("🛑 Shutting down client.")
            break
    socket.close()
    context.term()

if __name__ == "__main__":
    
    main()
