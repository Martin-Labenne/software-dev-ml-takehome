import polars as pl
from tempfile import NamedTemporaryFile

from typing import Union, IO, Generator

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

def _pl_scan_csv(
    path_or_buf: FilePath | IO[bytes] | IO[str], 
    **kwargs
) -> pl.LazyFrame: 

    return pl.scan_csv(
        path_or_buf, 
        has_header=False,
        new_columns=['player_id', 'match_id', 'operator_id', 'nb_kills'],
        schema_overrides=[pl.String, pl.String, pl.Categorical, pl.UInt8],
        **kwargs 
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
            yield _pl_scan_csv(
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
        return _pl_scan_csv(path_or_buf)
    else: 
        return _scan_matches_iter_chunks(path_or_buf, chunksize)


def store_tempfile(df:pl.DataFrame) -> str: 
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        temp_file_path = temp_file.name
        df.write_csv(file=temp_file_path, include_header=True)
    
    return temp_file_path

