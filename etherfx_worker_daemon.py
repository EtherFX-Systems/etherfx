import daemon
import dill
import importlib
import json
import lockfile
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
        print(task_metadata)
        function = self.prop_function(task_metadata["module"]+self.xstr(task_metadata["_class"]), task_metadata["function"])
        if not function: return

        arguments = gds.get_args_from_gds(task_metadata["task_id"])
        if not arguments: return

        result = function(*(map(dill.loads, arguments)))
        gds.set_result_in_gds(task_metadata["task_id"], dill.dumps(result))

    def prop_function(self, module, function):
        """imports a Python function"""
        if not module: return
        if not function: return

        try:
            imported_module = importlib.import_module(module)
            return get_attr(imported_module, function)
        except ModuleNotFoundError as e:
            return
        # else:
        #    custom_function(function, args)

    def custom_function(function, args):
        raise NotImplementedError
        # function = dill.loads(function)

    def xstr(self, s):
        return '' if s is None else ("." + str(s))


def main():
    with daemon.DaemonContext(
        stdout=sys.stdout,
        stderr=sys.stderr
    ):
        app = DaemonApp(logging_enabled=True)
        app.run()


if __name__ == "__main__":
    main()
