import bisect
import logging
import os
from common import fruit_item, message_protocol, middleware


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}  # { id_client: [FruitItem, ...] }


    def _process_data(self, id_client, fruit, amount):
        logging.info(f"Processing data message for client {id_client}")
        if id_client not in self.fruit_top:
            self.fruit_top[id_client] = []

        client_top = self.fruit_top[id_client]

        for i in range(len(client_top)):
            if client_top[i].fruit == fruit:
                client_top[i] = client_top[i] + fruit_item.FruitItem(fruit, amount)
                return

        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, id_client):
        logging.info(f"Received EOF for client {id_client}")

        client_top = self.fruit_top.pop(id_client, [])

        fruit_chunk = list(client_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda item: (item.fruit, item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize([id_client] + fruit_top)
        )

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            id_client, fruit, amount = fields
            self._process_data(id_client, fruit, amount)
        elif len(fields) == 1:
            id_client = fields[0]
            self._process_eof(id_client)

        ack()


    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()

    return 0


if __name__ == "__main__":
    main()