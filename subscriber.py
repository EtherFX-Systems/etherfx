import pika
import json
import logging

logging.basicConfig(level=logging.DEBUG)

class Subscriber():

    def __init__(self, driver):
        self.__driver = driver


    def create_channel(self):
        connection = self.__driver.create_connection('127.0.0.1')
        channel = connection.channel()
        channel.queue_declare(queue='demo_publishing_queue')

        channel.exchange_declare(
            exchange="demo_exchange",
            exchange_type="direct",
            passive=False,
            durable=True,
            auto_delete=False)

        channel.basic_consume(self.callback,
                      queue='demo_publishing_queue',
                      no_ack=True)

        channel.start_consuming()
        # connection.close()

    def callback(self, ch, method, properties, body):
        task_meta = json.loads(body)
        task_id = task_meta["task_id"]

        
if __name__ == "__main__":
    subscriber = Subscriber(Driver())
    subscriber.create_channel()
    