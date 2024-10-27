from pathlib import Path
from uuid import uuid4
import polars as pl
import numpy as np

from src.constants import OPERATORS, R6_MATCHES_LOG_LOCATION, PREVIOUS_DAYS, R6_MATCHES_STATS
from src.helpers import scan_matches
from src.queries import operator_top_100, match_top_10

def get_today(): 
    return '20241027'

def generate_dummy_daily_results():
    r6_logs_path = Path(R6_MATCHES_LOG_LOCATION) 
    lazy_df = scan_matches(r6_logs_path)
    top_100_avg_kills_dummy = operator_top_100(lazy_df).collect()
    top_10_matches_dummy = match_top_10(lazy_df).collect()

    op_100_folder = 'data/daily/operator_top_100/'
    match_10_folder = 'data/daily/match_top_10/'

    def _replace_match_id(df): 
        match_ids = df["match_id"].unique().to_list()
        mapping = { match_id: str(uuid4()) for match_id in match_ids }
        return (
            df.with_columns(match_id=pl.col("match_id").replace(mapping))
        ) 

    def _store_sub_result(path, df): 
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            df.write_csv(file=path, include_header=True)

    for day in PREVIOUS_DAYS: 
        op_path = Path(f'{op_100_folder}{day}.csv')
        match_path = Path(f'{match_10_folder}{day}.csv')

        _store_sub_result(op_path, _replace_match_id(top_100_avg_kills_dummy))
        _store_sub_result(match_path, _replace_match_id(top_10_matches_dummy))



def generate_corrupted_rows(df: pl.DataFrame, corruption_ratio: float = 0.001) -> pl.DataFrame: 
    num_rows = df.shape[0]
    num_corrupted = int(num_rows * corruption_ratio)
    
    corrupted_indices = np.random.choice(num_rows, num_corrupted, replace=False)
    corruption_types = np.random.randint(1, 12, size=num_corrupted)

    corruped_df = df
    
    for corrupted_indice, corruption_type in list(zip(corrupted_indices, corruption_types)):
        idx = int(corrupted_indice)
        
        match int(corruption_type): 
            case 1:  
                corruped_df[idx, 'player_id'] = "not-a-uuid"  
            case 2:  
                corruped_df[idx, 'match_id'] = "not-a-uuid"
            case 3:  
                corruped_df[idx, 'operator_id'] = "bad_op_id"  
            case 4:  
                corruped_df[idx, 'nb_kills'] = -1  
            case 5: 
                corruped_df[idx, 'nb_kills'] = 2000
            case 6: 
                corruped_df[idx, 'nb_kills'] = 2.5
            case 7: 
                corruped_df[idx, 'nb_kills'] = 'kills'
            case 8: 
                corruped_df[idx, 'player_id'] = None
            case 9: 
                corruped_df[idx, 'match_id'] = None
            case 10: 
                corruped_df[idx, 'operator_id'] = None
            case 11: 
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
    
    match_ids = np.empty(total_rows, dtype='U36')  # For UUID match_ids
    player_ids = np.empty(total_rows, dtype='U36')  # For UUID player_ids
    operator_ids = np.empty(total_rows, dtype=np.int32)
    nb_kills = np.empty(total_rows, dtype=np.int32)

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

        match_ids[current_idx:current_idx + match_nb_of_rows] = match_id
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

    corruped_matches_df = generate_corrupted_rows(matches_df, corruption_ratio)

    return corruped_matches_df
