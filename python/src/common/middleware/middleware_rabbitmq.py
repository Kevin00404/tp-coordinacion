import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError
)

PREFETCH_COUNT=20

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()

        self._queue_name = queue_name
        self._channel.queue_declare(queue=self._queue_name)

        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        self._consuming = False

    def start_consuming(self, on_message_callback): 
        def internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=internal_callback,
                auto_ack=False
            )

            self._consuming = True
            self._channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err

        finally:
            self._consuming = False
    
    def stop_consuming(self):
        if not self._consuming or not self._channel or not self._channel.is_open:
            return

        try:
            self._channel.stop_consuming()
            self._consuming = False

        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message
            )

        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err

    def close(self):
        try:
            if self._consuming and self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._consuming = False

            if self._channel and self._channel.is_open:
                self._channel.close()

            if self._connection and self._connection.is_open:
                self._connection.close()

        except Exception as err:
            raise MessageMiddlewareCloseError(str(err)) from err


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys

        self._consuming = False
        
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='topic')
        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)


    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        queue_info = self._channel.queue_declare(queue='', exclusive=True)
        bound_queue = queue_info.method.queue

        for r_key in self._routing_keys:
            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=bound_queue,
                routing_key=r_key
            )

        self._channel.basic_consume(
            queue=bound_queue,
            on_message_callback=callback,
            auto_ack=False
        )

        self._consuming = True
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err
        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err
        finally:
            self._consuming = False


    def stop_consuming(self):
        if not self._consuming or not self._channel or not self._channel.is_open:
            return

        try:
            self._channel.stop_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err
        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err


    def send(self, message):
        try:
            for key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=key,
                    body=message
                )
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err

    def close(self):
        try:
            if self._consuming and self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._consuming = False

            if self._channel and self._channel.is_open:
                self._channel.close()

            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as err:
            raise MessageMiddlewareCloseError(str(err)) from err