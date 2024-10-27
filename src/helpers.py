import polars as pl
import re
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import Union, IO, Generator

from src.constants import OPERATORS, R6_MATCHES_STATS

FilePath = Union[str, bytes] 
    
def store_matchs(
    path_or_buf: FilePath | IO[bytes] | IO[str], 
    df: pl.DataFrame
) -> None : 
    df.write_csv(file=path_or_buf, include_header=False)
    
def store_daily_result(
    path_or_buf: FilePath | IO[bytes] | IO[str], 
    df: pl.DataFrame
) -> None : 
    df.write_csv(file=path_or_buf, include_header=True)



def _lazy_validation(df: pl.LazyFrame) -> pl.LazyFrame:
    uuid_v4_pattern = r'^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$'
    return df.filter(
        (pl.col('player_id').is_not_null()) &
        (pl.col('match_id').is_not_null()) &
        (pl.col('operator_id').is_not_null()) &
        (pl.col('nb_kills').is_not_null()) &
        (pl.col('player_id').str.count_matches(uuid_v4_pattern) == 1) &
        (pl.col('match_id').str.count_matches(uuid_v4_pattern) == 1) &
        (pl.col('operator_id').is_in(OPERATORS)) &
        (pl.col('nb_kills').is_between(R6_MATCHES_STATS['MIN_NB_KILLS'], R6_MATCHES_STATS['MAX_NB_KILLS']))
    )








def _scan_csv(
    path_or_buf: FilePath | IO[bytes] | IO[str], 
    **kwargs
) -> pl.LazyFrame: 

    return _lazy_validation(
        pl.scan_csv(
            path_or_buf, 
            has_header=False,
            new_columns=['player_id', 'match_id', 'operator_id', 'nb_kills'],
            schema_overrides=[pl.String, pl.String, pl.UInt8, pl.UInt8],
            truncate_ragged_lines=True,
            ignore_errors=True,
            **kwargs 
        )
    ) 

def _scan_matches_iter_chunks(
    path_or_buf: FilePath | IO[bytes] | IO[str], 
    chunksize: int
) -> Generator[pl.LazyFrame, None, None] :
    if chunksize < 1:
        raise ValueError("Chunk size must be a positive integer greater than zero.") 

    do_continue = True
    chunk_nb = 0
    while do_continue: 
        try:
            yield _scan_csv(
                path_or_buf,
                skip_rows=chunk_nb*chunksize,
                n_rows=chunksize
            )
            chunk_nb += 1
        except pl.exceptions.NoDataError: 
            do_continue = False

def scan_matches(
    path_or_buf: FilePath | IO[bytes] | IO[str]
    , chunksize: int = None
) -> pl.LazyFrame | Generator[pl.LazyFrame, None, None]: 
    if chunksize is None: 
        return _scan_csv(path_or_buf)
    else: 
        return _scan_matches_iter_chunks(path_or_buf, chunksize)


def store_tempfile(df:pl.DataFrame) -> str: 
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        temp_file_path = temp_file.name
        df.write_csv(file=temp_file_path, include_header=True)
    
    return temp_file_path

def store_format_operator_top_100(path: Path, df: pl.DataFrame): 
    if not path.parent.exists(): 
        path.parent.mkdir(parents=True, exist_ok=True)
        
    pre_format = df.group_by('operator_id').agg([
        pl.col('match_id'), 
        pl.col('nb_kills') 
    ])

    with path.open(mode='a') as file: 
        for operator_id, match_id, nb_kills in pre_format.iter_rows():
            match_kills_pairs = list(zip(match_id, nb_kills))
            match_kills_strings = [ f'{match}:{kills}' for match, kills in match_kills_pairs ]

            file.write(f'{operator_id}|{','.join(match_kills_strings)}\n')

def store_format_match_top_10(path: Path, df: pl.DataFrame): 
    if not path.parent.exists(): 
        path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(mode='a') as file: 
        for match_id, nb_kills in df.iter_rows():
            match_kills_string = f'{match_id}:{nb_kills}\n'

            file.write(match_kills_string)


def get_last_seven_files(dir_path: Path): 
    files = list(dir_path.glob('*')) 
    files.sort()
    
    last_seven_files = files[-7:]
    
    return last_seven_files