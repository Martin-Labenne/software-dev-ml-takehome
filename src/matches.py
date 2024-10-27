import numpy as np
import polars as pl
from uuid import uuid4
from typing import Generator, IO
from pathlib import Path

from src.constants import OPERATORS, R6_MATCHES_STATS

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
    path: Path, 
    **kwargs
) -> pl.LazyFrame: 

    return _lazy_validation(
        pl.scan_csv(
            path, 
            has_header=False,
            new_columns=['player_id', 'match_id', 'operator_id', 'nb_kills'],
            schema_overrides=[pl.String, pl.String, pl.UInt8, pl.UInt8],
            truncate_ragged_lines=True,
            ignore_errors=True,
            **kwargs 
        )
    ) 

def _scan_matches_iter_chunks(
    path: Path, 
    chunksize: int
) -> Generator[pl.LazyFrame, None, None] :
    if chunksize < 1:
        raise ValueError("Chunk size must be a positive integer greater than zero.") 

    do_continue = True
    chunk_nb = 0
    while do_continue: 
        try:
            yield _scan_csv(
                path,
                skip_rows=chunk_nb*chunksize,
                n_rows=chunksize
            )
            chunk_nb += 1
        except pl.exceptions.NoDataError: 
            do_continue = False

def scan_matches(
    path: Path,
    chunksize: int = None
) -> pl.LazyFrame | Generator[pl.LazyFrame, None, None]: 
    if chunksize is None: 
        return _scan_csv(path)
    else: 
        return _scan_matches_iter_chunks(path, chunksize)
    
def store_matches(
    path: Path | IO, 
    df: pl.DataFrame
) -> None : 
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(file=path, include_header=False)

def _generate_corrupted_rows(df: pl.DataFrame, corruption_ratio: float = 0.001) -> pl.DataFrame: 
    if corruption_ratio == 0: 
        return df
    
    num_rows = df.shape[0]
    num_corrupted = int(num_rows * corruption_ratio)
    
    corrupted_indices = np.random.choice(num_rows, num_corrupted, replace=False)
    corruption_types = np.random.randint(1, 10, size=num_corrupted)

    corruped_df = df
    
    for corrupted_indice, corruption_type in list(zip(corrupted_indices, corruption_types)):
        idx = int(corrupted_indice)
        
        match int(corruption_type): 
            case 1:  
                corruped_df[idx, 'player_id'] = "not-a-uuid"  
            case 2:  
                corruped_df[idx, 'match_id'] = "not-a-uuid"
            case 3:  
                corruped_df[idx, 'operator_id'] = 0
            case 4:  
                corruped_df[idx, 'nb_kills'] = -1  
            case 5: 
                corruped_df[idx, 'nb_kills'] = 200
            case 6: 
                corruped_df[idx, 'player_id'] = None
            case 7: 
                corruped_df[idx, 'match_id'] = None
            case 8: 
                corruped_df[idx, 'operator_id'] = None
            case 9: 
                corruped_df[idx, 'nb_kills'] = None
            
    return corruped_df

def generate_matches(n_matches:int=1000, corruption_ratio: float = 0.001) -> pl.DataFrame: 
    """
    Generate a DataFrame containing simulated match data.

    This function creates a DataFrame representing match records, including player IDs, match IDs,
    operator IDs, and the number of kills for each entry. The number of matches is specified by 
    the user, and the function randomly generates additional data based on defined distributions 
    and parameters.

    After generation, the matches are shuffled. 

    Parameters:
    -----------
    n_matches : int
        The number of matches to simulate. Each match will have a varying number of rows 
        (gameplay entries) based on a normal distribution, constrained within specified 
        boundaries.

    Returns:
    --------
    pl.DataFrame
        A polars DataFrame containing the following columns:
        - 'player_id': A unique identifier for each player involved in the matches.
        - 'match_id': A unique identifier for each match played.
        - 'operator_id': The operator chosen by the player during the match.
        - 'nb_kills': The number of kills achieved by the player in that match.

    Notes:
    ------
    - If n_matches is less than or equal to zero, an empty DataFrame is returned with the defined 
      column names.
    - The number of players available for selection is calculated as a ratio of the number of matches,
      with a minimum of 10 players.
    - The function generates match records with an average number of rows per match set at 55.833,
      with a standard deviation of approximately 9.33. The number of rows per match is clipped 
      between a minimum of 25 and a maximum of 82 to ensure reasonable gameplay scenarios.
    - Player IDs are generated as UUIDs, while operator IDs are randomly selected from a predefined
      list of operator values.
    - **Warning**: Generating a very high number of matches (e.g., 1,000,000) may lead to high memory usage,
      potentially causing your system to freeze, especially on machines with limited RAM (e.g., 8 GB). 
      It is recommended to test with smaller values first and considere batch generation.

    Example:
    ---------
    >>> df = generate_matches(1000)
    >>> print(df.head())
    """
    if n_matches <= 0: 
        return pl.DataFrame({'player_id': [], 'match_id': [], 'operator_id': [], 'nb_kills': []}) 
    
    nb_players_per_match = 10
    nb_players_ratio = 0.1  # 100/1000
    nb_players = max(nb_players_per_match, round(nb_players_ratio * n_matches))

    players = [ str(uuid4()) for _ in range(nb_players) ] 
    matches = [ str(uuid4()) for _ in range(n_matches) ] 

    operators = np.array(OPERATORS)

    match_nb_of_rows_all = np.clip(
        np.random.normal(R6_MATCHES_STATS['AVG_NB_ROWS_PER_MATCH'], R6_MATCHES_STATS['STD_NB_ROWS_PER_MATCH'], size=n_matches), 
        R6_MATCHES_STATS['NB_ROWS_LOW_BOUNDARY'], 
        R6_MATCHES_STATS['NB_ROWS_HIGH_BOUNDARY']
    ).astype(int)

    total_rows = match_nb_of_rows_all.sum()
    
    match_ids = np.empty(total_rows, dtype='U36') # For UUID match_ids
    player_ids = np.empty(total_rows, dtype='U36')  # For UUID player_ids
    operator_ids = np.empty(total_rows, dtype=np.uint8)
    nb_kills = np.empty(total_rows, dtype=np.uint8)

    def _get_match_players(i): 
        # Calculate the start index using modulo to wrap around
        start_idx = (i * 10) % nb_players
        end_idx = start_idx + 10

        # If the end index exceeds nb_players, wrap around to the beginning
        if end_idx > nb_players:
            match_players = players[start_idx:] + players[:end_idx % nb_players]
        else:
            match_players = players[start_idx:end_idx]

        return match_players
    

    current_idx = 0
    
    for i, match_id in enumerate(matches): 
        match_nb_of_rows = match_nb_of_rows_all[i]
        match_players = _get_match_players(i)

        sequence_players = np.random.choice(match_players, size=match_nb_of_rows, replace=True) 
        
        sequence_operators = np.random.choice(operators, size=match_nb_of_rows, replace=True)
        sequence_nb_kills = np.random.randint(0, 5, size=match_nb_of_rows)

        match_ids[current_idx:current_idx + match_nb_of_rows] = np.repeat(match_id, match_nb_of_rows)
        player_ids[current_idx:current_idx + match_nb_of_rows] = sequence_players
        operator_ids[current_idx:current_idx + match_nb_of_rows] = sequence_operators
        nb_kills[current_idx:current_idx + match_nb_of_rows] = sequence_nb_kills

        current_idx += match_nb_of_rows

    matches_df = pl.DataFrame({
        'player_id': player_ids,
        'match_id': match_ids,
        'operator_id': operator_ids,
        'nb_kills': nb_kills
    }).sample(fraction=1, shuffle=True)

    corruped_matches_df = _generate_corrupted_rows(matches_df, corruption_ratio)

    return corruped_matches_df
