import src.daily_processor as processor
from src.misc import get_last_seven_files, store_format_operator_top_100, store_format_match_top_10
from src.constants import TODAY
from src.queries import merge_results_operator_top_100, merge_results_match_top_10

import polars as pl
from pathlib import Path

LOG_PATH = Path('data/logs/r6-matches.log')
CHUNK_SIZE = 10**4

RESULT_DIR = 'data/rolling_seven_days/'

partition_map = processor.partition_log_file(LOG_PATH, CHUNK_SIZE)

processor.store_daily_operator_top_100(
    processor.compute_daily_operator_top_100(partition_map),
    TODAY
)

processor.store_daily_match_top_10(
    processor.compute_daily_match_top_10(partition_map),
    TODAY
) 

dir_daily_operator_top_100 = Path('data/daily/operator_top_100/')
dir_daily_match_top_10 = Path('data/daily/match_top_10/')

last_seven_files_operator_top_100 = get_last_seven_files(dir_daily_operator_top_100)
last_seven_files_match_top_10 = get_last_seven_files(dir_daily_match_top_10)

rolling_seven_days_operator_top_100 = merge_results_operator_top_100(
    [ pl.scan_csv(file) for file in last_seven_files_operator_top_100 ]
).collect()

rolling_seven_days_match_top_10 = merge_results_match_top_10(
    [ pl.scan_csv(file) for file in last_seven_files_match_top_10 ]
).collect()

store_format_operator_top_100(Path(f'{RESULT_DIR}operator_top100_{TODAY}.txt'), rolling_seven_days_operator_top_100)
store_format_match_top_10(Path(f'{RESULT_DIR}match_top10_{TODAY}.txt'), rolling_seven_days_match_top_10)