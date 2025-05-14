import asyncio
from time import time, sleep
import numpy as np


async def little_fun():
    num = np.random.randint(1,5)
    await asyncio.sleep(num)
    print('ready', num)


async def fun():
    semaphore = asyncio.Semaphore(5)

    async def worker():
        async with semaphore:
            await little_fun()

    tasks = [worker() for i in range(10)]
    await asyncio.gather(*tasks)


asyncio.run(fun())
