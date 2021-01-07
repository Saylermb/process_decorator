import unittest
import asyncio
from time import time

from process_decorator import async_process


def e_func1():
    return 2 ** 1000000001


def e_func2():
    return 2 ** 1000000000


def e_func3():
    return 2 ** 20000001


def e_func4():
    return 2 ** 20000002


@async_process()
def func1():
    return 2 ** 1000000001


@async_process()
def func2():
    return 2 ** 1000000000


@async_process(exit_process_timer=5)
def func3():
    return 2 ** 20000001


@async_process(exit_process_timer=5)
def func4():
    return 2 ** 20000002


class ProcessDecorator(unittest.TestCase):

    async def _test_async_one_time_process_call(self):
        res = await asyncio.gather(func1(), func2())
        return res

    def test_async_one_time_process_call(self):
        t = time()
        res = asyncio.run(self._test_async_one_time_process_call())
        print(f'time exec with decorator {time() - t}')
        t = time()
        e_func1()
        e_func2()
        print(f'time exec without decorator {time() - t}')
        self.assertEqual(res, [e_func1(), e_func2()])

    async def _test_async_process_call(self):
        res = await asyncio.gather(func3(), func4())
        return res

    def test_async_process_call(self):
        t = time()
        for _ in range(100):
            res = asyncio.run(self._test_async_process_call())
        print(f'time exec with decorator {time() - t}')
        t = time()
        for _ in range(100):
            e_func3()
            e_func4()
        print(f'time exec without decorator {time() - t}')
        self.assertEqual(res, [e_func3(), e_func4()])
