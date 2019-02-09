from daemon import runner
import dill
import importlib
import time
import etherfx-core

class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/tmp/foo.pid'
        self.pidfile_timeout = 5

    def run(self):
        while True:
            pollRabbitMQ()

    def performTask(TaskId):
        data = retrieveDataFromGDS(taskId)
        function = propfunction(data)
        exec(function)

    def retrieveDataFromGDS(taskId):
        etherfx-core.datastore.getTask()
        # return path, klass, function, args
        # propFunction

    def propfunction(path, klass, function, args):
        """Constructs a python function"""
        if function is None:
            libraryFunction(path, klass, function, args)
        else:
            customFunction(function, args)

    def customFunction(function, args):
        function = dill.loads(function)

    def libraryFunction(module, function, args):
        function = importlib.import_module(module, function)

def main():
    app = App()
    daemon_runner = runner.DaemonRunner(app)
    daemon_runner.do_action()
    exit()

if __name__ == '__main__':
    main()