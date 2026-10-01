import os
import logging
import signal

from common import fruit_item, middleware, message_protocol


MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops = {}  # { id_client: { fruit: FruitItem } }
        self.partial_top_count = {}  # { id_client: cantidad de aggregations }

    def stop(self):
        logging.info("Stopping JoinFilter")
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.warning("Failed to stop Join input queue: %s", exc)

        try:
            self.input_queue.close()
            self.output_queue.close()
        except Exception as exc:
            logging.warning("Failed to close Join middleware: %s", exc)

    def process_messsage(self, message, ack, nack):
        logging.info("Received top in Join")
        fields = message_protocol.internal.deserialize(message)
        id_client = fields[0]
        partial_top = fields[1:]

        if id_client not in self.partial_tops:
            self.partial_tops[id_client] = {}
            self.partial_top_count[id_client] = 0

        client_fruits = self.partial_tops[id_client]
        for fruit, amount in partial_top:
            current = client_fruits.get(fruit, fruit_item.FruitItem(fruit, 0))
            client_fruits[fruit] = current + fruit_item.FruitItem(fruit, amount)

        self.partial_top_count[id_client] += 1
        if self.partial_top_count[id_client] < AGGREGATION_AMOUNT:
            ack()
            return

        final_top = sorted(client_fruits.values(), reverse=True)[:TOP_SIZE]
        result = [(item.fruit, item.amount) for item in final_top]
        self.output_queue.send(
            message_protocol.internal.serialize([id_client] + result)
        )
        del self.partial_tops[id_client]
        del self.partial_top_count[id_client]
        ack()

    def start(self):
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: self.stop(),
        )
        self.input_queue.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0

if __name__ == "__main__":
    main()