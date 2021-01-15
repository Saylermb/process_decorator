import _queue
import asyncio
import pickle
from functools import wraps
from multiprocessing import get_context
from random import randint
from time import time, sleep
import zmq.asyncio

from process_decorator.zmq_backend.zmq_register import run_zmq_server

_GLOB_FUNC_QUEUE_STORAGE = {}

ctx = get_context()


async def _func_process(func, input_zmq, output_zmq, exit_timer):
    is_coro = asyncio.iscoroutinefunction(func)
    timer = time()
    sender, receiver = _register_zmq(input_zmq, output_zmq)
    while timer + exit_timer > time():
        print('get')
        args, kwargs = pickle.loads(await receiver.recv())
        print('start execute')
        result = func(*args, **kwargs)
        if is_coro:
            result = await result
        print('complited', type(result))
        await sender.send((pickle.PickleBuffer(pickle.dumps(result, protocol=-1))))
        print('send')


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
        print('create new ')
        sender, r, p = _create_process(func, exit_process_timer)
    else:
        sender, r, p = queue
    print('send args')
    await sender.send((pickle.PickleBuffer(pickle.dumps((args, kwargs), protocol=-1))))
    print('send and reciv')
    result = await r.recv()
    print('return', result)
    sleep(1)
    return pickle.loads(result)


async def _one_time_func_process(func, output, args, kwargs):
    is_coro = asyncio.iscoroutinefunction(func)
    result = func(*args, **kwargs)
    if is_coro:
        result = await result
    output.put(result)


def _one_time_run_func_process(func, output, args, kwargs):
    return asyncio.run(_one_time_func_process(func, output, args, kwargs))


def _run_func_process(func, input, out, exit_timer):
    return asyncio.run(_func_process(func, input,out, exit_timer))


async def _create_one_time_process(func, args, kwargs):
    queue = ctx.Queue(1)
    p = ctx.Process(target=_one_time_run_func_process, args=(func, queue, args, kwargs))
    p.start()
    while True:
        try:
            result = queue.get_nowait()
            break
        except _queue.Empty:
            await asyncio.sleep(.00001)
    queue.close()
    return result

def async_process(exit_process_timer=0):
    def inner_function(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if exit_process_timer > 0:
                return await _async_process_with_cache(func, exit_process_timer, args, kwargs)
            else:
                return await _create_one_time_process(func, args, kwargs)

        return wrapper

    return inner_function
