# import daemon
import dill
import importlib
import json
# import lockfile
import logging
import os
import sys
import time
from core.redis_interface import GDSClient
from core.rabbitmq_interface import RabbitMQInterface

class DaemonApp:
    def __init__(self, logging_enabled=False):
        self.gds = GDSClient()
        self.message_queue = RabbitMQInterface()
        self.logger = logging.getLogger('etherfx-worker')

        if logging_enabled:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.CRITICAL)

    def run(self):
        self.logger.debug("Starting EtherFx daemon")
        self.message_queue.subscribe_to_queue(self.callback)

    def callback(self, channel, method_frame, header_frame, body):
        self.logger.debug("Callback function called")
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return self.perform_task(json.loads(body))

    def perform_task(self, task_metadata):
        self.logger.debug("Received task metadata for task_id {}", task_metadata["task_id"])
        # print(task_metadata)
        function = self.prop_function(task_metadata["module"]+self.xstr(task_metadata["_class"]), task_metadata["function"])
        if not function:
            self.logger.debug("No function {}.{} for task_id: {}".format(
                task_metadata["module"]+self.xstr(task_metadata["_class"]),
                task_metadata["function"],
                task_metadata["task_id"]
            ))
            return

        arguments = self.gds.get_args_from_gds(task_metadata["task_id"])
        if not arguments:
            self.logger.debug("No args for task_id {}".format(task_metadata["task_id"]))
            return

        deserialized_args = [dill.loads(x) for x in arguments]

        try:
            result = function(*(deserialized_args))
        except Exception as e:
            result = e

        self.logger.debug("Result: {}".format(result))
        self.logger.debug("Serialized Result: ")
        # print(dill.dumps(result))
        self.gds.set_result_in_gds(task_metadata["task_id"], [dill.dumps(result)])
        return result

    def prop_function(self, module, function):
        if not module: return
        if not function: return

        try:
            imported_module = importlib.import_module(module)
            return getattr(imported_module, function)
        except ModuleNotFoundError as e:
            self.logger.debug("No function {}.{}".format(module, function))
            return
        # else:
        #    custom_function(function, args)

    def custom_function(self, function, args):
        raise NotImplementedError
        # function = dill.loads(function)

    def xstr(self, s):
        if s is None or s =='':
            return ''
        else:
            return ("." + str(s))


def main():
    # with daemon.DaemonContext(
    #     stdout=sys.stdout,
    #     stderr=sys.stderr
    # ):
    app = DaemonApp(logging_enabled=True)
    app.run()


if __name__ == "__main__":
    main()
