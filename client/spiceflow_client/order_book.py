from spiceflow_client.gen_templates import OrderBook


def deserialise_and_print(buf: bytearray) -> None:
    order_book = OrderBook.OrderBook.GetRootAsOrderBook(buf)
    deserialised_message = {
                "topic": order_book.Topic().decode(),
                "bids": (order_book.Bids(0).Price(), order_book.Bids(0).Size()),
                "asks": (order_book.Asks(0).Price(), order_book.Asks(0).Size())
            }
    print(deserialised_message)
