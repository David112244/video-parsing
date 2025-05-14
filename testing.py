import subprocess
import re
from glob import glob
import pandas as pd
import numpy as np
from multiprocessing import Pool
import os
import asyncio
from time import time
import aiofiles.os as async_os


def clean_vtt(filepath):
    with open(filepath, encoding='utf-8') as f:
        lines = f.readlines()

    # Определяем язык
    lang = None
    for line in lines:
        m = re.match(r'Language:\s*([a-zA-Z\-]+)', line)
        if m:
            lang = m.group(1)
            break

    # Собираем только текстовые строки
    text_lines = []
    for line in lines:
        # Пропускаем служебные строки, таймкоды, пустые строки
        if line.strip() == '' or \
                line.startswith('WEBVTT') or \
                line.startswith('Kind:') or \
                line.startswith('Language:') or \
                re.match(r'\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line) or \
                'align:' in line or 'position:' in line:
            continue
        # Удаляем inline таймкоды <00:00:13.160> и html-теги
        clean = re.sub(r'<.*?>', '', line)
        clean = clean.strip()
        if clean:
            text_lines.append(clean)

    # Удаляем дубликаты подряд и объединяем в текст
    result = []
    prev = None
    for line in text_lines:
        if line != prev:
            result.append(line)
        prev = line

    transcript = ' '.join(result)
    return lang, transcript


def search_id(str_):
    pattern = r'\[([^\]]+)\]'

    match = re.search(pattern, str_)
    if match:
        content = match.group(1)
        return content
    else:
        print("Совпадений не найдено")


def clean_files(batch_num):  # raw
    raw_paths = f'data/raw_subtitles/batch_{batch_num}/*'
    for folder_path in glob(raw_paths):
        for raw_path in glob(f'{folder_path}/*'):
            lang, text = clean_vtt(raw_path)
            # print(f"Language: {lang}\n{text[:1000]}...\n")
            video_id = search_id(raw_path)
            new_folder_path = f'data/clean_subtitles/batch_{batch_num}/{video_id}'
            os.makedirs(new_folder_path, exist_ok=True)
            clean_path = f'{new_folder_path}/{lang}.txt'
            with open(clean_path, 'w', encoding='utf-8') as file:
                file.write(text)


async def async_glob(path_pattern):
    return await asyncio.to_thread(glob, path_pattern)


async def get_transcription(pack):
    start_time = time()
    video_id, batch_num = pack
    # main_command = 'yt-dlp --write-auto-subs --sub-lang ru,en --skip-download "<URL>"'
    url = f'https://www.youtube.com/watch?v={video_id}'
    folder = f'data/raw_subtitles/batch_{batch_num}/{video_id}'
    await async_os.makedirs(folder, exist_ok=True)
    if len(await async_glob(f'{folder}/*')) == 2:  # если субтитры отсутствуют, на пост обработке будет видно что папка пуста
        print('skip')
        return
    commands = [f'cd {folder}',
                f'yt-dlp --write-auto-subs --sub-lang ru,en --skip-download "{url}"']
    process = await asyncio.create_subprocess_shell(
        f'yt-dlp --write-auto-subs --sub-lang ru,en --skip-download "{url}"',
        cwd=folder,  # <--- Важно!
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )

    await process.wait()
    print(f'Поиск субтитров видео {video_id} был выполнен за {np.round(start_time - time(), 2)} секунд')


def get_subtitles():
    batch_size = 1000
    all_ids = pd.read_csv('data/all_ids.csv').iloc[:, 0]
    for i in range(len(all_ids) // batch_size + 1):
        start, stop = i * batch_size, (i + 1) * batch_size
        to_function = [[id_, i] for id_ in all_ids[start:stop]]
        for _ in range(2):
            with Pool(processes=5) as pool:
                pool.map(get_transcription, to_function[:])
        for pack in to_function[:]:
            get_transcription(pack)


async def get_subtitles_async():
    batch_size = 4
    processes_count = 4
    all_ids = pd.read_csv('data/all_ids.csv').iloc[:, 0][:20]
    semaphore = asyncio.Semaphore(processes_count)

    async def worker(pack):
        async with semaphore:
            await get_transcription(pack)

    for i in range(len(all_ids) // batch_size + 1):
        start, stop = i * batch_size, (i + 1) * batch_size
        to_function = [worker([id_, i]) for id_ in all_ids[start:stop]]
        await asyncio.gather(*to_function[:])
        clean_files(i)


if __name__ == '__main__':
    print('Запуск')
    for _ in range(3):
        asyncio.run(get_subtitles_async())
