import core
import daemon
import dill
import importlib
import lockfile
import os
import sys
import time

class DaemonApp:
    def run(self):
        while True:
            poll_rabbit()

    def perform_task(TaskId):
        data = retrieve_data_from_gds(taskId)
        function = prop_function(data)
        exec(function)

    def retrieve_data_from_GDS(taskId):
        core.datastore.getTask()
        # return path, klass, function, args
        # propFunction

    def prop_function(path, klass, function, args):
        """Constructs a python function"""
        if function is None:
            library_function(path, klass, function, args)
        else:
            custom_function(function, args)

    def custom_function(function, args):
        function = dill.loads(function)

    def library_function(module, function, args):
        function = importlib.import_module(module, function)


def main():
    with daemon.DaemonContext(
        stdout=sys.stdout,
        stderr=sys.stderr,
        pidfile=lockfile.FileLock("/tmp/etherfx_worker_daemon.pid")
    ):
        print(os.getuid())
        print(os.getgid())


if __name__ == "__main__":
    main()
