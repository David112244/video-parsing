from async_collect_recomendation import collect_recommendations
# from put_together import put_together
# from transcription import run

import asyncio


if __name__ == '__main__': #вызываеть только через консоль
    import aiofiles

    print('Запуск')
    start_time = asyncio.get_event_loop().time()
    asyncio.run(collect_recommendations())
    print(f'Код выполнен за {round(asyncio.get_event_loop().time() - start_time, 2)} секунд')
    # put_together()
    # run()