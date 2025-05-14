# данный файл будет собирать рекомендации. Url будет брать из пути data/video_urls/batch_0.csv
# сохранять в папку data/raw_recommended/batch_0/id.csv
# хранить url (id) с которых уже собрали рекомендации в папке data/raw_checked_ids/batch_0/id.csv
# собирать в одно место все проверенные id в папку data/checked_ids/batch_0.csv


import pandas as pd
import numpy as np
from glob import glob
import os
import re
import requests
import json
from time import time, sleep
from multiprocessing import Pool


def extract_video_id(url: str) -> str:
    patterns = [
        r'(?:v=|\/)([0-9A-Za-z_-]{11})',  # v=VIDEO_ID или /VIDEO_ID
        r'youtu\.be\/([0-9A-Za-z_-]{11})',  # короткий URL
        r'embed\/([0-9A-Za-z_-]{11})'  # embed URL
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def get_related_videos_from_request(id, depth=2, count_video=10):
    def parse_recommendations(data: dict) -> list:
        """Парсит JSON структуру для получения рекомендаций"""
        recommendations = []

        try:
            items = data['contents']['twoColumnWatchNextResults']['secondaryResults'][
                'secondaryResults']['results']

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

        return recommendations[:10]

    def extract_yt_initial_data(html: str) -> dict:
        """Извлекает ytInitialData из HTML-кода"""
        pattern = re.compile(r'var\s+ytInitialData\s*=\s*({.*?});', re.DOTALL)
        match = pattern.search(html)
        return json.loads(match.group(1)) if match else {}

    class YouTubePersonalizedParser:
        def __init__(self):
            self.session = requests.Session()
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9'
            }

        def _watch_video(self, video_id: str, watch_sec: int = 3):
            """Имитирует просмотр видео для генерации истории"""
            try:
                # Запрос страницы видео
                self.session.get(
                    f'https://www.youtube.com/watch?v={video_id}',
                    headers=self.headers
                )

                # Имитация времени просмотра
                watch_sec = np.random.randint(3, 10)
                sleep(watch_sec)

            except Exception as e:
                print(f"Watch error: {str(e)}")

        def get_personalized_recommendations(self, video_id: str, depth: int = 2) -> list:
            """
            Генерирует персонализированные рекомендации через эмуляцию поведения

            :param seed_url: Стартовое видео
            :param depth: Глубина эмуляции просмотров (2-3 оптимально)
            :return: Персонализированные рекомендации
            """
            result_list = []
            try:
                # Первичный просмотр
                self._watch_video(video_id)

                # # Эмуляция цепочки просмотров
                # for _ in range(depth):
                #     recs = self.get_recommendations(video_id)
                #     result_list.append([r['url'] for r in recs])
                #     if not recs:
                #         break
                #
                #     # Выбираем случайную рекомендацию
                #     video_id = extract_video_id(recs[0]['url'])
                #     self._watch_video(video_id, watch_sec=10)
                #
                #     #версия для каждой рекомендации
                #     for r in recs:
                #         video_id = extract_video_id(r['url'])
                #         self._watch_video(video_id, watch_sec=10)

                # экспериментальная часть
                recs = self.get_recommendations(video_id)
                result_list.append([r['url'] for r in recs])
                second_list = []
                for r in recs:
                    video_id = extract_video_id(r['url'])
                    self._watch_video(video_id, watch_sec=10)
                    second_recs = self.get_recommendations(video_id)
                    second_list.append([sr['url'] for sr in second_recs])
                result_list.append(second_list)

                # Финалные рекомендации
                # return self.get_recommendations(video_id)
                return result_list

            except Exception as e:
                print(f"Error: {str(e)}")
                return []

        def get_recommendations(self, video_id: str) -> list:
            """Основная функция получения рекомендаций с сохранением сессии"""
            try:
                response = self.session.get(
                    f'https://www.youtube.com/watch?v={video_id}',
                    headers=self.headers
                )

                data = extract_yt_initial_data(response.text)
                return parse_recommendations(data)

            except Exception as e:
                print(f"Request error: {str(e)}")
                return []

    parser = YouTubePersonalizedParser()
    recommendations = parser.get_personalized_recommendations(
        id,
        depth=2
    )
    return recommendations


def inner_function(pack):
    start_time = time()
    try:
        video_id, batch_num = pack
        recommendations = get_related_videos_from_request(video_id)
        father_id = video_id
        first_r = recommendations[0]  # shape 10
        second_r = recommendations[1]  # shape 10x10
        df = pd.DataFrame(columns=['url', 'father_url', 'deep', 'vnp'])
        for fr in first_r:
            line = [extract_video_id(fr), father_id, 1, 'by']
            df.loc[len(df)] = line

        for i, fr in enumerate(first_r):
            for sr in second_r[i]:
                line = [extract_video_id(sr), extract_video_id(fr), 2, 'by']
                df.loc[len(df)] = line
        df.to_csv(f'data/raw_recommended/batch_{batch_num}/{video_id}.csv', index=False)
        with open(f'data/raw_checked_ids/batch_{batch_num}/{video_id}.txt', 'w') as file:
            pass
        print(f'Рекомендации полученны за {np.round(start_time - time())} секунд')
    except Exception as e:
        print(e)


def collect_recommendations():
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
            checked_ids_from_raw = [path.split('\\')[-1].split('.')[0] for path in glob(path_to_raw_check)]
            pd.Series(checked_ids_from_raw).to_csv(path_to_checked_ids, index=False)

        try:
            checked_ids = pd.read_csv(path_to_checked_ids).iloc[:, 0]
        except:
            checked_ids = pd.Series()
        unchecked_ids = list(set(video_ids) - set(checked_ids))
        to_pool = [[un_id, batch_num] for un_id in unchecked_ids]

        with Pool(processes=1) as pool:
            pool.map(inner_function, to_pool[:1])

        for path in glob(path_to_raw_rec):
            rec = pd.read_csv(path)
            if len(rec) < 100:  # брак
                video_id = path.split('\\')[-1].split('.')[0]
                os.remove(path)
                os.remove(f'data/raw_checked_ids/batch_{batch_num}/{video_id}.txt')


if __name__ == '__main__':
    print('Запуск')
    start_time = time()
    collect_recommendations()
    print(f'Код выполнен за {np.round(start_time - time())}')
