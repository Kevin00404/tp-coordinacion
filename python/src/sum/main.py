import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"]) # BORRAR: identificador de cada instancia de SUM
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"]) # cant. de nodos sum que hay.
SUM_PREFIX = os.environ["SUM_PREFIX"] # SUM_PREFIX = sum
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE" 
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# crear un nuevo exhange s

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        self.sum_control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}# dicc estuctutra: { id_client: { fruit: FruitItem } }

    def _process_data(self, id_client, fruit, amount):#Simplemente va creando diccionario o actualizando amount d ecada furta. NO envia por cola o exhange
        logging.info(f"Process data for client {id_client}")
        
        if id_client not in self.amount_by_fruit:
            self.amount_by_fruit[id_client] = {}
        client_fruits = self.amount_by_fruit[id_client]

        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))


    def _process_eof(self, id_client):
        self.sum_control_output.send(
            message_protocol.internal.serialize([id_client])
        )

    def _send_partial_result(self, id_client):
        logging.info(f"Broadcasting data messages for client {id_client}")

        client_fruits = self.amount_by_fruit.pop(id_client, {})

        for fruit, final_fruit_item in client_fruits.items():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [id_client, fruit, final_fruit_item.amount]
                    )
                )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([id_client])
            )


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(fields[0])
        ack() 

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._send_partial_result(fields[0])
        ack()

    def start(self):
        control_thread = threading.Thread(
            target=self.sum_control_exchange.start_consuming,
            args=(self.process_control_message,),
            daemon=True,
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)# TODO: stop consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter() 
    sum_filter.start() 
    return 0


if __name__ == "__main__":
    main()
