from queries import partition_by_match_prefix, operator_top_100, match_top_10, merge_results_operator_top_100, merge_results_match_top_10
from helpers import scan_matches, store_tempfile, store_daily_result
import polars as pl
from pathlib import Path

def partition_log_file(log_path, chunksize=10**7): 

    chunked_partition_map = {}
    partition_map = {}

    for lazy_chunk in scan_matches( log_path, chunksize ):
        # lazy load with column creation
        partitionned_chunk = partition_by_match_prefix(lazy_chunk).collect()

        # partitions are stored in temp file
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

def _partition_apply(partition_map, function): 
    lazy_map = { key: '' for key in partition_map.keys() }

    for idx, partition_path in partition_map.items(): 
        partition = pl.scan_csv(partition_path)
        partition_result = function(partition)

        lazy_map[idx] = partition_result

    return lazy_map


def compute_daily_operator_top_100(partition_map):

    lazy_map = _partition_apply(partition_map, operator_top_100)

    lazy_result = merge_results_operator_top_100(
        [ partition_operator_top_100 for partition_operator_top_100 in lazy_map.values() ]
    )

    return lazy_result.collect()


def compute_daily_match_top_10(partition_map):

    lazy_map = _partition_apply(partition_map, match_top_10)

    lazy_result = merge_results_match_top_10(
        [ partition_match_top_10 for partition_match_top_10 in lazy_map.values() ]
    )

    return lazy_result.collect()


def store_daily_operator_top_100(df, str_date):
    path = Path(
        f'{Path(__file__).parent}/../data/daily/operator_top_100/{str_date}'
    )
    operator_top_100_dir = path.parent
    if not operator_top_100_dir.exists(): 
        operator_top_100_dir.mkdir(parents=True, exist_ok=True)

    store_daily_result(path, df)


def store_daily_match_top_10(df, str_date): 
    path = Path(
        f'{Path(__file__).parent}/../data/daily/match_top_10/{str_date}'
    )
    match_top_10_dir = path.parent
    if not match_top_10_dir.exists(): 
        match_top_10_dir.mkdir(parents=True, exist_ok=True)

    store_daily_result(path, df)


