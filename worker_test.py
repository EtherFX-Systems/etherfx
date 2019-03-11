import dill
import etherfx_worker_daemon as worker
from mock import patch, Mock
from numpy.linalg import inv as np_linalg_inv
from numpy import array as np_array
from numpy import array_equal
from core.redis_interface import GDSClient

app = worker.DaemonApp(logging_enabled=False)

def test_prop_function(module, function):
    global app
    function = app.prop_function(module, function)
    assert function == np_linalg_inv

def fake_get_args_from_gds(self, task_id):
    return [dill.dumps(np_array([[1., 2.], [3., 4.]]))]

def fake_set_result_in_gds(self, task_id, execution_result):
    return execution_result

@patch.object(GDSClient, 'get_args_from_gds', fake_get_args_from_gds)
@patch.object(GDSClient, 'set_result_in_gds', fake_set_result_in_gds)
def test_perform_task(task_metadata):
    global app
    assert array_equal(app.perform_task(task_metadata), np_linalg_inv(([[1., 2.], [3., 4.]]))) == True

def main():
    global app

    task_metadata = {
        "module": "numpy",
        "function": "inv",
        "args": 1,
        "kwargs": None,
        "task_id": 123,
        "_class": "linalg"
    }

    test_prop_function(task_metadata["module"]+app.xstr(task_metadata["_class"]), task_metadata["function"])

    test_perform_task(task_metadata)

if __name__ == "__main__":
    main()