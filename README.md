## process_decorator

If you write async code, you now that all code work on one processor (tnx GIL), but we can resolve this problem
with execution code in other process.

ATTENTION, we get profit only if function work more than 0.1 sec or if you need make async function.
AND don't work global variable, because func work in other process
```python
from process_decorator import async_process
import asyncio

@async_process()
def test():
    return 2*500000000

if __name__ == '__main__':
    asyncio.run(test())
```
this code creates other process where will be executed. 

**async_process** get argument exit_process_timer, witch cache process on time executed.
If you call this function every time, this parameter will up speed of execution
```python
from process_decorator import async_process
import asyncio

@async_process(exit_process_timer=10)
def test():
    return 2*50000000

if __name__ == '__main__':
    for _ in range(30):
        asyncio.run(test())
```
```text
where:

exit_process_timer - is time in second when process close if wont call
```
