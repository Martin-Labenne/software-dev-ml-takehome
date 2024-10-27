import polars as pl
import argparse
from pathlib import Path

from src import daily_processor as processor
from src.misc import get_last_seven_files, store_format_operator_top_100, store_format_match_top_10
from src.constants import TODAY
from src.queries import merge_results_operator_top_100, merge_results_match_top_10

DEFAULT_LOG_PATH = Path('data/logs/r6-matches.log')
DEFAULT_CHUNK_SIZE = 10**4
RESULT_DIR = Path('data/rolling_seven_days/')
DIR_DAILY_OPERATOR_TOP_100 = Path('data/daily/operator_top_100/')
DIR_DAILY_MATCH_TOP_10 = Path('data/daily/match_top_10/')

def process_daily_log(log_path: Path, chunk_size: int):
    partition_map = processor.partition_log_file(log_path, chunk_size)
    
    # Daily operator and match processing
    operator_top_100 = processor.compute_daily_operator_top_100(partition_map)
    processor.store_daily_operator_top_100(operator_top_100, TODAY)
    
    match_top_10 = processor.compute_daily_match_top_10(partition_map)
    processor.store_daily_match_top_10(match_top_10, TODAY)

def update_rolling_seven_days():

    last_seven_files_operator_top_100 = get_last_seven_files(DIR_DAILY_OPERATOR_TOP_100)
    last_seven_files_match_top_10 = get_last_seven_files(DIR_DAILY_MATCH_TOP_10)
    
    rolling_seven_days_operator_top_100 = merge_results_operator_top_100(
        [pl.scan_csv(file) for file in last_seven_files_operator_top_100]
    ).collect()
    
    rolling_seven_days_match_top_10 = merge_results_match_top_10(
        [pl.scan_csv(file) for file in last_seven_files_match_top_10]
    ).collect()
    
    store_format_operator_top_100(
        RESULT_DIR / f'operator_top100_{TODAY}.txt', rolling_seven_days_operator_top_100
    )
    store_format_match_top_10(
        RESULT_DIR / f'match_top10_{TODAY}.txt', rolling_seven_days_match_top_10
    )


def main(): 
    parser = argparse.ArgumentParser(description="Process daily log and update rolling seven-day stats.")
    parser.add_argument('--log-path', type=Path, default=DEFAULT_LOG_PATH, help="Path to the log file.")
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE, help="Chunk size for log file processing.")
    
    args = parser.parse_args()
    
    process_daily_log(args.log_path, int(args.chunk_size))
    
    update_rolling_seven_days()

if __name__ == '__main__': 
    main()