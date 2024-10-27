import polars as pl
from pathlib import Path
from typing import Callable, Dict

from src.queries import partition_by_match_prefix, operator_top_100, match_top_10, merge_results_operator_top_100, merge_results_match_top_10
from src.misc import store_tempfile
from src.matches import scan_matches
from src.daily_results import store_daily_result

def partition_log_file(log_path: Path, chunksize:int =10**7) -> Dict[str, str] : 
    """
    Partition a large log file into temporary files based on the match ID prefix. Each partition corresponds to
    a unique match prefix to facilitate efficient, prefix-based processing of the data.

    Parameters:
    -----------
    log_path : Path
        Path to the main log file that will be partitioned.
    chunksize : int, optional
        Number of rows per chunk when reading the log file in batches. Default is 10 million rows.

    Returns:
    --------
    Dict[str, str]
        Dictionary mapping each unique match prefix to the path of its corresponding temporary file.
    """
    chunked_partition_map = {}
    partition_map = {}

    for lazy_chunk in scan_matches( log_path, chunksize ):
        
        partitionned_chunk = partition_by_match_prefix(lazy_chunk).collect()

        for match_prefix, player_id, match_ids, operator_id, nb_kills in partitionned_chunk.iter_rows(): 
            if not (match_prefix in chunked_partition_map) : 
                chunked_partition_map[match_prefix] = []
            
            tempfile_path = store_tempfile(
                pl.DataFrame({
                    'player_id': player_id,
                    'match_id': match_ids,
                    'operator_id': operator_id,
                    'nb_kills': nb_kills
                })
            )
            chunked_partition_map[match_prefix].append(tempfile_path)

    for match_prefix, paths in chunked_partition_map.items(): 
        lazy_concat = pl.concat(
            [ pl.scan_csv(path) for path in paths ],
            how='vertical'
        )
        tempfile_path = store_tempfile(lazy_concat.collect())
        partition_map[match_prefix] = tempfile_path

    return partition_map

def _partition_apply(partition_map: Dict[str, str], function: Callable) -> Dict[str, pl.LazyFrame]: 
    """ Apply a function to each partition in the partition map. """
    lazy_map = { key: '' for key in partition_map.keys() }

    for idx, partition_path in partition_map.items(): 
        partition = pl.scan_csv(partition_path)
        partition_result = function(partition)

        lazy_map[idx] = partition_result

    return lazy_map


def compute_daily_operator_top_100(partition_map: Dict[str, str]) -> pl.DataFrame:
    lazy_map = _partition_apply(partition_map, operator_top_100)

    lazy_result = merge_results_operator_top_100(
        [ partition_operator_top_100 for partition_operator_top_100 in lazy_map.values() ]
    )

    return lazy_result.collect()


def compute_daily_match_top_10(partition_map: Dict[str, str]) -> pl.DataFrame:
    lazy_map = _partition_apply(partition_map, match_top_10)

    lazy_result = merge_results_match_top_10(
        [ partition_match_top_10 for partition_match_top_10 in lazy_map.values() ]
    )

    return lazy_result.collect()


def store_daily_operator_top_100(df: pl.DataFrame, str_date: str):
    path = Path(
        f'{Path(__file__).parent}/../data/daily/operator_top_100/{str_date}.csv'
    )
    operator_top_100_dir = path.parent
    if not operator_top_100_dir.exists(): 
        operator_top_100_dir.mkdir(parents=True, exist_ok=True)

    store_daily_result(path, df)


def store_daily_match_top_10(df: pl.DataFrame, str_date: str): 
    path = Path(
        f'{Path(__file__).parent}/../data/daily/match_top_10/{str_date}.csv'
    )
    match_top_10_dir = path.parent
    if not match_top_10_dir.exists(): 
        match_top_10_dir.mkdir(parents=True, exist_ok=True)

    store_daily_result(path, df)


