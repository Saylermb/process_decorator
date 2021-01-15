import _queue
import asyncio
from asyncio import sleep
from functools import wraps
from multiprocessing import get_context
from time import time


_GLOB_FUNC_QUEUE_STORAGE = {}

ctx = get_context()
async def _func_process(func, input: ctx.Queue, output, exit_timer):
    is_coro = asyncio.iscoroutinefunction(func)
    timer = time()
    while exit_timer < 0 or timer+exit_timer > time():
        try:
            args, kwargs = input.get_nowait()
        except _queue.Empty:
            await sleep(.0001)
            continue
        result = func(*args, **kwargs)
        if is_coro:
            result = await result
        output.put(result)
        timer = time()
    input.close()
    output.close()


async def _one_time_func_process(func, output, args, kwargs):
    is_coro = asyncio.iscoroutinefunction(func)
    result = func(*args, **kwargs)
    if is_coro:
        result = await result
    output.put(result)


def _one_time_run_func_process(func, output, args, kwargs):
    return asyncio.run(_one_time_func_process(func, output, args, kwargs))


def _run_func_process(func, input, output, exit_timer):
    return asyncio.run(_func_process(func, input, output, exit_timer))


def _create_process(func, exit_timer):
    global _GLOB_FUNC_QUEUE_STORAGE
    queue_input = ctx.Queue(8)
    queue_output = ctx.Queue(4)
    p = ctx.Process(target=_run_func_process, args=(func, queue_input, queue_output, exit_timer))
    p.start()
    _GLOB_FUNC_QUEUE_STORAGE[str(func)] = queue_input, queue_output, p
    return queue_input, queue_output, p


async def _create_one_time_process(func, args, kwargs):
    queue = ctx.Queue(1)
    p = ctx.Process(target=_one_time_run_func_process, args=(func, queue, args, kwargs))
    p.start()
    while True:
        try:
            result = queue.get_nowait()
            break
        except _queue.Empty:
            await asyncio.sleep(.0001)
    queue.close()
    return result


async def _async_process_with_cache(func, exit_process_timer, args, kwargs):
    queue = _GLOB_FUNC_QUEUE_STORAGE.get(str(func))
    if queue is None or not queue[-1].is_alive():
        queue_input, queue_output, p = _create_process(func, exit_process_timer)
    else:
        queue_input, queue_output, p = queue
    queue_input.put((args, kwargs))
    while p.is_alive():
        try:
            result = queue_output.get_nowait()
            return result
        except _queue.Empty:
            await sleep(.00001)
    raise RuntimeError(f'process {p} was close')


def async_process(exit_process_timer=0):
    def inner_function(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if exit_process_timer != 0:
                return await _async_process_with_cache(func, exit_process_timer, args, kwargs)
            else:
                return await _create_one_time_process(func, args, kwargs)
        return wrapper
    return inner_function


