import unittest
import asyncio
from time import time

from process_decorator import async_process

@async_process()
async def async_test(*args):
    await asyncio.sleep(.1)
    return args

@async_process(10)
async def async_test_with_cache(*args, **kwargs):
    await asyncio.sleep(.1)
    return args

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

@async_process(exit_process_timer=5)
async def func5(i):
    asyncio.sleep(1)
    return i


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

    def test_async_func(self):
        res = asyncio.run(async_test(1,2,3))
        self.assertEqual(res, (1,2,3))
        res = asyncio.run(async_test_with_cache(1, 2, 3))
        self.assertEqual(res, (1, 2, 3))
        res = asyncio.run(async_test_with_cache(1, 2, 3))
        self.assertEqual(res, (1, 2, 3))

    async def _test_delivery(self):
        return await asyncio.gather(func5(1), func5(2),func5(3),func5(4))

    def test_delivery(self):
        for _ in range(10):
            res = asyncio.run(self._test_delivery())
            self.assertEqual(res, (1, 2, 3, 4))