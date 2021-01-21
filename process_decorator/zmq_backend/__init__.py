import asyncio
import pickle
from asyncio import sleep
from functools import wraps
from multiprocessing import get_context
import zmq.asyncio

from process_decorator.zmq_backend.zmq_register import run_zmq_server

_GLOB_FUNC_QUEUE_STORAGE = {}

ctx = get_context()


async def _func_process(func, input_zmq, output_zmq):
    is_coro = asyncio.iscoroutinefunction(func)
    sender, receiver = _register_zmq(input_zmq, output_zmq)
    while True:
        args, kwargs = pickle.loads(await receiver.recv())
        result = func(*args, **kwargs)
        if is_coro:
            result = await result
        await sender.send((pickle.PickleBuffer(pickle.dumps(result, protocol=-1))))


def _register_zmq(zmq_in: str, zmq_out):
    """
    :param _type: zmq.REQ zmq.REP
    :return:
    """
    context = zmq.asyncio.Context()
    sender = context.socket(zmq.PUSH)
    sender.connect(zmq_in)
    receiver = context.socket(zmq.PULL)
    receiver.connect(zmq_out)
    return sender, receiver


def _create_process(func, exit_timer):
    global _GLOB_FUNC_QUEUE_STORAGE
    _, zmq_main_in, zmq_main_out = run_zmq_server()
    _, zmq_child_in, zmq_child_out = run_zmq_server()
    p = ctx.Process(target=_run_func_process, args=(func, zmq_child_in, zmq_main_out, exit_timer))
    p.start()
    s, r = _register_zmq(zmq_main_in, zmq_child_out)
    _GLOB_FUNC_QUEUE_STORAGE[str(func)] = s, r, p
    return s, r, p


async def _async_process_with_cache(func, exit_process_timer, args, kwargs):
    queue = _GLOB_FUNC_QUEUE_STORAGE.get(str(func))
    if queue is None:
        print('get new')
        sender, r, p = _create_process(func, exit_process_timer)
    else:
        print('get old')
        sender, r, p = queue
    await sender.send((pickle.PickleBuffer(pickle.dumps((args, kwargs), protocol=-1))))
    result = await r.recv()
    return pickle.loads(result)


def _run_func_process(func, input, out, exit_timer):
    return asyncio.run(_func_process(func, input,out))


def async_process(exit_process_timer=0):
    def inner_function(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if exit_process_timer != 0:
                return await _async_process_with_cache(func, exit_process_timer, args, kwargs)
            else:
                raise NotImplementedError

        return wrapper

    return inner_function
