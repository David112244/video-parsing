import asyncio
import numpy as np

async def inner_fun(num):
    await asyncio.sleep(1)
    print(f'string: {num}')


count_async = 2
semaphore = asyncio.Semaphore(count_async)

async def worker(num):
    async with semaphore:
        await inner_fun(num)

async def run():
    arr = np.arange(10)
    to_function = [await worker(n) for n in arr]
    await asyncio.gather(*to_function)

if __name__ == '__main__':
    asyncio.run(run())