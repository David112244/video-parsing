import pandas as pd
import numpy as np
import aiohttp
import asyncio
import aiofiles
import os
import re
import json
from glob import glob

def extract_video_id(url: str) -> str:
    patterns = [
        r'(?:v=|\/)([0-9A-Za-z_-]{11})',
        r'youtu\.be\/([0-9A-Za-z_-]{11})',
        r'embed\/([0-9A-Za-z_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def parse_recommendations(data: dict) -> list:
    recommendations = []
    try:
        items = data['contents']['twoColumnWatchNextResults']['secondaryResults']['secondaryResults']['results']
        for item in items:
            if 'compactVideoRenderer' in item:
                video = item['compactVideoRenderer']
                title = video['title']['accessibility']['accessibilityData']['label']
                video_id = video['videoId']
                recommendations.append({
                    'title': title.replace('\n', ' ').strip(),
                    'url': f'https://youtu.be/{video_id}'
                })
    except KeyError as e:
        print(f"KeyError in JSON structure: {str(e)}")
    return recommendations[:2]

def extract_yt_initial_data(html: str) -> dict:
    pattern = re.compile(r'var\s+ytInitialData\s*=\s*({.*?});', re.DOTALL)
    match = pattern.search(html)
    return json.loads(match.group(1)) if match else {}

class AsyncYouTubePersonalizedParser:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }

    async def _watch_video(self, session, video_id: str):
        try:
            async with session.get(
                f'https://www.youtube.com/watch?v={video_id}',
                headers=self.headers
            ) as response:
                await response.text()
            await asyncio.sleep(np.random.randint(3, 10))
        except Exception as e:
            print(f"Watch error: {str(e)}")

    async def get_recommendations(self, session, video_id: str) -> list:
        try:
            async with session.get(
                f'https://www.youtube.com/watch?v={video_id}',
                headers=self.headers
            ) as response:
                html = await response.text()
                data = extract_yt_initial_data(html)
                return parse_recommendations(data)
        except Exception as e:
            print(f"Request error: {str(e)}")
            return []

    async def get_personalized_recommendations(self, session, video_id: str) -> list:
        result_list = []
        try:
            await self._watch_video(session, video_id)
            recs = await self.get_recommendations(session, video_id)
            result_list.append([r['url'] for r in recs])
            second_list = []
            for r in recs:
                vi = extract_video_id(r['url'])
                await self._watch_video(session, vi)
                second_recs = await self.get_recommendations(session, vi)
                second_list.append([sr['url'] for sr in second_recs])
            result_list.append(second_list)
            return result_list
        except Exception as e:
            print(f"Error: {str(e)}")
            return []

async def inner_function(pack):
    await asyncio.sleep(0.1)
    video_id, batch_num = pack
    print(f'Обработка {video_id}')
    start_time = asyncio.get_event_loop().time()
    parser = AsyncYouTubePersonalizedParser()
    async with aiohttp.ClientSession() as session:
        recommendations = await parser.get_personalized_recommendations(session, video_id)
    if not recommendations or len(recommendations) < 2:
        print(f'Нет рекомендаций для {video_id}')
        return
    father_id = video_id
    first_r = recommendations[0]
    second_r = recommendations[1]
    df = pd.DataFrame(columns=['url', 'father_url', 'deep', 'vnp'])
    for fr in first_r:
        line = [extract_video_id(fr), father_id, 1, 'by']
        df.loc[len(df)] = line
    for i, fr in enumerate(first_r):
        for sr in second_r[i]:
            line = [extract_video_id(sr), extract_video_id(fr), 2, 'by']
            df.loc[len(df)] = line
    os.makedirs(f'data/raw_recommended/batch_{batch_num}', exist_ok=True)
    os.makedirs(f'data/raw_checked_ids/batch_{batch_num}', exist_ok=True)
    df.to_csv(f'data/raw_recommended/batch_{batch_num}/{video_id}.csv', index=False)
    async with aiofiles.open(f'data/raw_checked_ids/batch_{batch_num}/{video_id}.txt', 'w') as file:
        await file.write('')
    elapsed = round(asyncio.get_event_loop().time() - start_time, 2)
    print(f'Рекомендации для {video_id} получены за {elapsed} секунд')

async def collect_recommendations():
    print('collect rec')
    batch_count = 20
    for batch_num in range(batch_count):
        path_to_video_urls = f'data/video_urls/batch_{batch_num}.csv'
        path_to_checked_ids = f'data/checked_ids/batch_{batch_num}.csv'
        path_to_raw_check = f'data/raw_checked_ids/batch_{batch_num}/*'
        path_to_raw_rec = f'data/raw_recommended/batch_{batch_num}/*'

        video_urls = pd.read_csv(path_to_video_urls).iloc[:, 0]
        video_ids = [extract_video_id(url) for url in video_urls]
        try:
            checked_ids = pd.read_csv(path_to_checked_ids).iloc[:, 0]
        except:
            checked_ids = pd.Series()
        if len(checked_ids) < len(glob(path_to_raw_check)):
            checked_ids_from_raw = [os.path.splitext(os.path.basename(path))[0] for path in glob(path_to_raw_check)]
            pd.Series(checked_ids_from_raw).to_csv(path_to_checked_ids, index=False)
        try:
            checked_ids = pd.read_csv(path_to_checked_ids).iloc[:, 0]
        except:
            checked_ids = pd.Series()
        unchecked_ids = list(set(video_ids) - set(checked_ids))
        to_pool = [[un_id, batch_num] for un_id in unchecked_ids]
        count_async = 5
        semaphore = asyncio.Semaphore(count_async)
        async def worker(pack):
            async with semaphore:
                await inner_function(pack)
        to_function = [worker(pack) for pack in to_pool[:10]]
        await asyncio.gather(*to_function)
        break
        for path in glob(path_to_raw_rec):
            rec = pd.read_csv(path)
            if len(rec) < 100:
                video_id = os.path.splitext(os.path.basename(path))[0]
                os.remove(path)
                os.remove(f'data/raw_checked_ids/batch_{batch_num}/{video_id}.txt')

if __name__ == '__main__':
    import aiofiles
    print('Запуск')
    start_time = asyncio.get_event_loop().time()
    asyncio.run(collect_recommendations())
    print(f'Код выполнен за {round(asyncio.get_event_loop().time() - start_time, 2)} секунд')

