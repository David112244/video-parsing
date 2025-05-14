from glob import glob
import pandas as pd

from related_functions import extract_video_id

def put_together():
    batch_count = 20

    id_fo_all_videos = None

    all_ids = []
    for batch_num in range(batch_count):
        path_to_raw_rec = f'data/raw_recommended/batch_{batch_num}/*'
        for path in glob(path_to_raw_rec):
            df = pd.read_csv(path)
            [all_ids.append(id_) for id_ in df['father_url']]
            [all_ids.append(id_) for id_ in df['url']]
    all_ids = list(set(all_ids))
    pd.Series(all_ids).to_csv('data/all_ids.csv', index=False)


