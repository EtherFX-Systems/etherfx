import pika, sys, os, logging
logging.basicConfig()

class Driver():

    def create_connection(self,host):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        return connection
        
    def put_into_progress_queue(self, TaskMetaData):
        return


if __name__ == "__main__":
    driver = Driver()
    driver.create_connection('127.0.0.1')

        
        


