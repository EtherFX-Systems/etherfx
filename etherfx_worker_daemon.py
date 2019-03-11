import daemon
import dill
import importlib
import lockfile
import os
import sys
import time
from core.redis_interface import GDSClient
from core.rabbitmq_interface import RabbitMQInterface

class DaemonApp:
    def __init__(self):
        self.gds = GDSClient()
        self.message_queue = RabbitMQInterface()

    def run(self):
        message_queue.subscribe_to_queue(callback)

    def callback(channel, method_frame, header_frame, body):
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return perform_task(body)

    def perform_task(task_metadata):
        function = prop_function(task_metadata["module"]+xstr(task_metadata["_class"]), task_metadata["function"])
        return if not function

        arguments = gds.get_args_from_gds(task_metadata["task_id"])
        return if not arguments

        result = function(*(map(dill.loads, arguments)))
        gds.set_result_in_gds(task_metadata["task_id"], dill.dumps(result))

    def prop_function(module, function):
        """imports a Python function"""
        return if not module
        return if not function

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

    def xstr(s):
        return '' if s is None else ("." + str(s))


def main():
    with daemon.DaemonContext(
        stdout=sys.stdout,
        stderr=sys.stderr,
        pidfile=lockfile.FileLock("/tmp/etherfx_worker_daemon.pid")
    ):
        app = DaemonApp()
        app.run()


if __name__ == "__main__":
    main()
