from driver import Driver
import pika
import json
import logging

logging.basicConfig(level=logging.DEBUG)

class Publisher():

    def __init__(self, driver):
        self.__driver = driver
        #need to change this payload once orchestrator is complete
        self.payload = {"task_id":"John", "module": "requested_module"}
        
    def create_channel(self):
        connection = self.__driver.create_connection('127.0.0.1')
        channel = connection.channel()

        channel.queue_declare(queue='demo_publishing_queue')

        channel.basic_publish(
            exchange= '',
            routing_key='demo_publishing_queue',
            body=json.dumps(self.payload))
        print(" Sent Message from Orchestrator")
        connection.close()

    # def publish_taskMetadata_to_queue_from_Orchestrator(self, TaskMetaData):
    #     self.payload = TaskMetaData

if __name__ == "__main__":
    publisher = Publisher(Driver())
    publisher.create_channel()
        